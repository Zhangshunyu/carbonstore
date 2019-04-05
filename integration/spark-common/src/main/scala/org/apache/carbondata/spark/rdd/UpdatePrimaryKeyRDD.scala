/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Date, Random, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, JobID, RecordReader, RecordWriter, TaskAttemptID, TaskID, TaskType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{ByteType, DataType, StringType}
import org.apache.spark.sql.util.SparkTypeConverter
import org.apache.spark.{Partition, TaskContext}

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, SegmentUpdateDetails}
import org.apache.carbondata.core.scan.executor.impl.MVCCVectorDetailQueryExecutor
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.scan.primarykey.PrimaryKeyDeleteVectorDetailQueryResultIterator
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.hadoop.{AbstractRecordReader, CarbonInputSplit, CarbonMultiBlockSplit, CarbonProjection, CarbonRecordReader, InputMetricsStats}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.{HandoffResult, PrimaryKeyResult}
import org.apache.carbondata.spark.util.CommonUtil



class UpdatePrimaryKeyResultIterator(
    recordReader: RecordReader[Void, Object],
    writer: RecordWriter[NullWritable, ObjectArrayWritable],
    dataTypes: Array[types.DataType]) extends CarbonIterator[Array[Object]] {
  val writable = new ObjectArrayWritable
  override def hasNext: Boolean = {
    recordReader.nextKeyValue()
  }

  override def next(): Array[Object] = {
    // TODO remove the copy
    val rowTmp = recordReader
      .getCurrentValue
      .asInstanceOf[InternalRow].copy()

    val row = new Array[Object](rowTmp.numFields)

    for (i <- 0 until row.length) {
      if (dataTypes(i) == StringType) {
        row(i) = rowTmp.getString(i).toString
      } else {
        row(i) = rowTmp.get(i, dataTypes(i))
      }
    }

    writable.set(row)
    writer.write(NullWritable.get, writable)
    row
  }
}

/**
 * execute streaming segment handoff
 */
class UpdatePrimaryKeyRDD[K, V](
    @transient private val ss: SparkSession,
    result: PrimaryKeyResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    handOffSegmentId: String) extends CarbonRDD[(K, V)](ss, Nil) {
  private val oldTimeStamp: Long = carbonLoadModel.getFactTimeStamp
  carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
  carbonLoadModel.setTaskNo((CarbonUpdateUtil.getLatestTaskIdForSegment(
    carbonLoadModel.getSegment, carbonLoadModel.getTablePath) + 1).toString)
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalCompute(
      split: Partition,
      context: TaskContext): Iterator[(K, V)] = {
    carbonLoadModel.setTaskNo((carbonLoadModel.getTaskNo.toLong + split.index).toString)
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
    // the input iterator is using raw row
    CommonUtil.setTempStoreLocation(split.index, carbonLoadModel, true, false)
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(getConf, attemptId)
    val dataTypes = new Array[DataType](carbonTable.getTableInfo.getFactTable.getListOfColumns.size())
    carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.foreach{d =>
      dataTypes(d.getSchemaOrdinal) = SparkTypeConverter.convertCarbonToSparkDataType(d, carbonTable)
    }

    val reader = prepareInputIterator(split, carbonTable, attemptContext)
    val writer = prepareRecordWriter

    // The iterator list here is unsorted. The sorted iterators are null.
    val iterator = new UpdatePrimaryKeyResultIterator(reader, writer, dataTypes)

    while (iterator.hasNext) {
      iterator.next()
    }
    reader.close()
    writer.close(attemptContext)
    var updateDetails = Seq[SegmentUpdateDetails]()
    val executor = reader.asInstanceOf[AbstractRecordReader[Object]].getQueryExecutor
    if (executor.isInstanceOf[MVCCVectorDetailQueryExecutor]) {
      val iterator = executor.asInstanceOf[MVCCVectorDetailQueryExecutor].getCarbonIterator
      if (iterator.isInstanceOf[PrimaryKeyDeleteVectorDetailQueryResultIterator]) {
        updateDetails =
          iterator.asInstanceOf[PrimaryKeyDeleteVectorDetailQueryResultIterator].
            getSegmentUpdateDetails.asScala

      }
    }

    new Iterator[(K, V)] {
      private var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey("" + finished, updateDetails)
      }
    }
  }

  /**
   * prepare input iterator by basing CarbonStreamRecordReader
   */
  private def prepareInputIterator(
      split: Partition,
      carbonTable: CarbonTable, attemptContext: TaskAttemptContextImpl
  ): RecordReader[Void, Object] = {
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    val hadoopConf = attemptContext.getConfiguration
    CarbonInputFormat.setDatabaseName(hadoopConf, carbonTable.getDatabaseName)
    CarbonInputFormat.setTableName(hadoopConf, carbonTable.getTableName)
    CarbonInputFormat.setTablePath(hadoopConf, carbonTable.getTablePath)
    val projection = new CarbonProjection
    val dataFields = carbonTable.getTableInfo.getFactTable.getListOfColumns
    val projectArray = new Array[String](dataFields.size())
    (0 until dataFields.size()).foreach { index =>
      projectArray(dataFields.get(index).getSchemaOrdinal) = dataFields.get(index).getColumnName
    }
    projectArray.foreach(projection.addColumn)
    CarbonInputFormat.setColumnProjection(hadoopConf, projection)
    CarbonInputFormat.setTableInfo(hadoopConf, carbonTable.getTableInfo)

    val format = new CarbonTableInputFormat[Array[Object]]()
    val model = format.createQueryModel(inputSplit, attemptContext)
    model.setUpdateTimeStamp(carbonLoadModel.getFactTimeStamp)
    model.setDirectVectorFill(false)
    val carbonRecordReader = createVectorizedCarbonRecordReader(model, null,
      "false")
    if (carbonRecordReader == null) {
      new CarbonRecordReader(model,
        format.getReadSupportClass(attemptContext.getConfiguration),
        null,
        attemptContext.getConfiguration)
    } else {
      carbonRecordReader
    }


    carbonRecordReader.initialize(inputSplit, attemptContext)
    carbonRecordReader
  }

  private def prepareRecordWriter: RecordWriter[NullWritable, ObjectArrayWritable] = {
    carbonLoadModel.setLoadWithoutConverterStep(true)
    carbonLoadModel.setFactTimeStamp(oldTimeStamp)
    CarbonTableOutputFormat.setLoadModel(getConf, carbonLoadModel)
    val format = new CarbonTableOutputFormat
    val jobId = new JobID(UUID.randomUUID.toString, 0)
    val random = new Random
    val task = new TaskID(jobId, TaskType.MAP, random.nextInt)
    val attemptID = new TaskAttemptID(task, random.nextInt)
    val context = new TaskAttemptContextImpl(getConf, attemptID)
    format.getRecordWriter(context)
  }

  def createVectorizedCarbonRecordReader(queryModel: QueryModel,
      inputMetricsStats: InputMetricsStats, enableBatch: String): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel, inputMetricsStats, enableBatch)
        .asInstanceOf[RecordReader[Void, Object]]
    } catch {
      case e: Exception =>

        null
    }
  }

  /**
   * get the partitions of the handoff segment
   */
  override protected def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val inputFormat = new CarbonTableInputFormat[Array[Object]]()
    val segmentList = new util.ArrayList[Segment](1)
    val result = new ArrayList[Partition]()
    segmentList.add(Segment.toSegment(handOffSegmentId, null))
    CarbonInputFormat.setTableName(job.getConfiguration, carbonLoadModel.getTableName)
    CarbonInputFormat.setDatabaseName(job.getConfiguration, carbonLoadModel.getDatabaseName)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
    job.getConfiguration.set(FileInputFormat.INPUT_DIR, carbonLoadModel.getTablePath)
    val splits = inputFormat.getSplits(job)
    val filteredSplit = splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).filter{carbonInputSplit =>
      FileFormat.ROW_V1 != carbonInputSplit.getFileFormat
    }
    CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, segmentList)
    CarbonInputFormat.setValidateSegmentsToAccess(job.getConfiguration, false)
    val newSplits = inputFormat.getSplits(job)

    val finalSplits = filteredSplit ++ newSplits.asScala.map(_.asInstanceOf[CarbonInputSplit])

    val groups = new ArrayBuffer[ArrayBuffer[CarbonInputSplit]]()
    finalSplits.foreach{s =>
      groups.find{f =>
        f.find(_.getSplitMerger.canBeMerged(
          s.getSplitMerger)) match {
          case Some(split) =>
            f += s
            true
          case _ => false
        }
      } match {
        case Some(p) =>
        case _ =>
          val splits = new ArrayBuffer[CarbonInputSplit]()
          splits += s
          groups += splits
      }
    }

    val regroups = new ArrayBuffer[ArrayBuffer[CarbonInputSplit]]()
    // Seperate the rowformats and columnar farmat groups.
    groups.foreach{ g =>
      if (g.exists(_.getVersion == ColumnarFormatVersion.R1) && g.size > 1) {
        regroups += g
      }
    }
    val logStr = new mutable.StringBuilder()
    regroups.zipWithIndex.foreach{ r =>
      logStr.append("group :" + r._2).append(" : ")
      r._1.foreach(s => logStr.append(s.getVersion + " : " +s.getBlockPath).append(" &&& ") )
      logStr.append(
        s"""
           | ----------------------
               """.stripMargin)
    }
    logInfo(logStr.toString())

    regroups.zipWithIndex.foreach { splitWithIndex =>
      val multiBlockSplit =
        new CarbonMultiBlockSplit(
          splitWithIndex._1.asJava,
          splitWithIndex._1.flatMap(f => f.getLocations).distinct.toArray)
      val partition = new CarbonSparkPartition(id, splitWithIndex._2, multiBlockSplit)
      result.add(partition)
    }

    result.asScala.toArray
  }
}
