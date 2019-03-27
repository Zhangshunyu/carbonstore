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

package org.apache.spark.keytable

import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.mapreduce.{Job, RecordReader, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.stream.CarbonStreamInputFormat
import org.apache.carbondata.spark.rdd.{CarbonRDD, StreamingRawResultIterator}

class StreamPartition(
    val rddId: Int,
    val idx: Int,
    @transient val inputSplit: CarbonInputSplit
) extends Partition {

  val split = new SerializableWritable[CarbonInputSplit](inputSplit)

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

class RawStreamDataScanRDD(
    @transient private val ss: SparkSession,
    table: CarbonTable,
    segments: util.List[Segment],
    readSortColumns: Boolean,
    projection: CarbonProjection = null
) extends CarbonRDD[Array[AnyRef]](ss, Nil) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new util.Date())
  }

  override def internalCompute(
      split: Partition,
      context: TaskContext): Iterator[Array[AnyRef]] = {
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
    val iterator = prepareInputIterator(split, table)
    new Iterator[Array[AnyRef]] {
      var notInvokeHasNext = true
      var preHasNext = false

      override def hasNext: Boolean = {
        if (notInvokeHasNext) {
          notInvokeHasNext = false
          preHasNext = iterator.hasNext
        }
        preHasNext
      }

      override def next(): Array[AnyRef] = {
        notInvokeHasNext = true
        iterator.next()
      }
    }
  }

  private def prepareInputIterator(
      split: Partition,
      carbonTable: CarbonTable
  ): RawResultIterator = {
    val inputSplit = split.asInstanceOf[StreamPartition].split.value
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val hadoopConf = getConf
    CarbonInputFormat.setDatabaseName(hadoopConf, carbonTable.getDatabaseName)
    CarbonInputFormat.setTableName(hadoopConf, carbonTable.getTableName)
    CarbonInputFormat.setTablePath(hadoopConf, carbonTable.getTablePath)
    if (projection == null) {
      val newProjection = new CarbonProjection
      val dataFields = carbonTable.getStreamStorageOrderColumn(carbonTable.getTableName)
      (0 until dataFields.size()).foreach { index =>
        newProjection.addColumn(dataFields.get(index).getColName)
      }
      CarbonInputFormat.setColumnProjection(hadoopConf, newProjection)
    } else {
      CarbonInputFormat.setColumnProjection(hadoopConf, projection)
    }
    CarbonInputFormat.setTableInfo(hadoopConf, carbonTable.getTableInfo)
    val attemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
    val format = new CarbonTableInputFormat[Array[AnyRef]]()
    val model = format.createQueryModel(inputSplit, attemptContext)
    val inputFormat = new CarbonStreamInputFormat
    inputFormat.setIsVectorReader(false)
    inputFormat.setModel(model)
    inputFormat.setUseRawRow(true)
    inputFormat.setReadSortColumns(readSortColumns)
    val streamReader = inputFormat.createRecordReader(inputSplit, attemptContext)
      .asInstanceOf[RecordReader[Void, Any]]
    streamReader.initialize(inputSplit, attemptContext)
    new StreamingRawResultIterator(streamReader)
  }

  @transient lazy val _partition: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val inputFormat = new CarbonTableInputFormat[Array[AnyRef]]()
    val splits = inputFormat.getSplitsOfStreaming(
      job,
      segments,
      table
    )
    (0 until splits.size()).map { index =>
      new StreamPartition(id, index, splits.get(index).asInstanceOf[CarbonInputSplit])
    }.toArray[Partition]
  }

  override protected def internalGetPartitions: Array[Partition] = _partition
}
