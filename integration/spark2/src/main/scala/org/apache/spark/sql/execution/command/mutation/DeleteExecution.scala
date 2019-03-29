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

package org.apache.spark.sql.execution.command.mutation

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, DeleteDeltaBlockDetails, SegmentUpdateDetails, TupleIdEnum}
import org.apache.carbondata.core.mutate.data.{BlockMappingVO, RowCountDetailsVO}
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.exception.MultipleMatchingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.DeleteDelataResultImpl
import org.apache.carbondata.spark.util.CarbonScalaUtil

object DeleteExecution {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * generate the delete delta files in each segment as per the RDD.
   * @return it gives the segments which needs to be deleted.
   */
  def deleteDeltaExecution(
      databaseNameOp: Option[String],
      tableName: String,
      sparkSession: SparkSession,
      dataRdd: RDD[Row],
      timestamp: String,
      isUpdateOperation: Boolean,
      executorErrors: ExecutionErrors): Seq[Segment] = {

    var res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors))]] = null
    val database = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val tablePath = absoluteTableIdentifier.getTablePath
    var segmentsTobeDeleted = Seq.empty[Segment]

    val deleteRdd = if (isUpdateOperation) {
      val schema =
        org.apache.spark.sql.types.StructType(Seq(org.apache.spark.sql.types.StructField(
          CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          org.apache.spark.sql.types.StringType)))
      val rdd = dataRdd
        .map(row => Row(row.get(row.fieldIndex(
          CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))))
      sparkSession.createDataFrame(rdd, schema).rdd
    } else {
      dataRdd
    }

    val (carbonInputFormat, job) = createCarbonInputFormat(absoluteTableIdentifier)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    val keyRdd = deleteRdd.map({ row =>
      val tupleId: String = row
        .getString(row.fieldIndex(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId)
      (key, row)
    }).groupByKey()

    // if no loads are present then no need to do anything.
    if (keyRdd.partitions.length == 0) {
      return segmentsTobeDeleted
    }
    val blockMappingVO =
      carbonInputFormat.getBlockRowCount(
        job,
        carbonTable,
        CarbonFilters.getPartitions(
          Seq.empty,
          sparkSession,
          TableIdentifier(tableName, databaseNameOp)).map(_.asJava).orNull, true)
    val segmentUpdateStatusMngr = new SegmentUpdateStatusManager(carbonTable)
    CarbonUpdateUtil
      .createBlockDetailsMap(blockMappingVO, segmentUpdateStatusMngr)

    val metadataDetails = SegmentStatusManager.readTableStatusFile(
      CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
    val isStandardTable = CarbonUtil.isStandardCarbonTable(carbonTable)
    val rowContRdd =
      sparkSession.sparkContext.parallelize(
        blockMappingVO.getCompleteBlockRowDetailVO.asScala.toSeq,
        keyRdd.partitions.length)

    val conf = SparkSQLUtil
      .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
    val rdd = rowContRdd.join(keyRdd)
    res = rdd.mapPartitionsWithIndex(
      (index: Int, records: Iterator[((String), (RowCountDetailsVO, Iterable[Row]))]) =>
        Iterator[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors))]] {
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
          var result = List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors))]()
          while (records.hasNext) {
            val ((key), (rowCountDetailsVO, groupedRows)) = records.next
            val segmentId = key.substring(0, key.indexOf(CarbonCommonConstants.FILE_SEPARATOR))
            result = result ++
                     deleteDeltaFunc(index,
                       key,
                       groupedRows.toIterator,
                       timestamp,
                       rowCountDetailsVO,
                       isStandardTable)
          }
          result
        }).collect()

    // if no loads are present then no need to do anything.
    if (res.flatten.isEmpty) {
      return segmentsTobeDeleted
    }

    // update new status file
    segmentsTobeDeleted = CarbonScalaUtil.checkAndUpdateStatusFiles(res,
      blockMappingVO,
      carbonTable,
      timestamp,
      executorErrors,
      isUpdateOperation)



    def deleteDeltaFunc(index: Int,
        key: String,
        iter: Iterator[Row],
        timestamp: String,
        rowCountDetailsVO: RowCountDetailsVO,
        isStandardTable: Boolean
    ): Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors))] = {

      val result = new DeleteDelataResultImpl()
      var deleteStatus = SegmentStatus.LOAD_FAILURE
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      // here key = segment/blockName
      val blockName = CarbonUpdateUtil
        .getBlockName(
          CarbonTablePath.addDataPartPrefix(key.split(CarbonCommonConstants.FILE_SEPARATOR)(1)))
      val segmentId = key.split(CarbonCommonConstants.FILE_SEPARATOR)(0)
      val deleteDeltaBlockDetails: DeleteDeltaBlockDetails = new DeleteDeltaBlockDetails(blockName)
      val resultIter = new Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors))] {
        val segmentUpdateDetails = new SegmentUpdateDetails()
        var TID = ""
        var countOfRows = 0
        try {
          while (iter.hasNext) {
            val oneRow = iter.next
            TID = oneRow
              .get(oneRow.fieldIndex(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).toString
            val offset = CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.OFFSET)
            val blockletId = CarbonUpdateUtil
              .getRequiredFieldFromTID(TID, TupleIdEnum.BLOCKLET_ID)
            val pageId = Integer.parseInt(CarbonUpdateUtil
              .getRequiredFieldFromTID(TID, TupleIdEnum.PAGE_ID))
            val IsValidOffset = deleteDeltaBlockDetails.addBlocklet(blockletId, offset, pageId)
            // stop delete operation
            if(!IsValidOffset) {
              executorErrors.failureCauses = FailureCauses.MULTIPLE_INPUT_ROWS_MATCHING
              executorErrors.errorMsg = "Multiple input rows matched for same row."
              throw new MultipleMatchingException("Multiple input rows matched for same row.")
            }
            countOfRows = countOfRows + 1
          }

          val blockPath =
            CarbonUpdateUtil.getTableBlockPath(TID, tablePath, isStandardTable)
          val completeBlockName = CarbonTablePath
            .addDataPartPrefix(CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCK_ID) +
                               CarbonCommonConstants.FACT_FILE_EXT)
          val deleteDeletaPath = CarbonUpdateUtil
            .getDeleteDeltaFilePath(blockPath, blockName, timestamp)
          val carbonDeleteWriter = new CarbonDeleteDeltaWriterImpl(deleteDeletaPath,
            FileFactory.getFileType(deleteDeletaPath))



          segmentUpdateDetails.setBlockName(blockName)
          segmentUpdateDetails.setActualBlockName(completeBlockName)
          segmentUpdateDetails.setSegmentName(segmentId)
          segmentUpdateDetails.setDeleteDeltaEndTimestamp(timestamp)
          segmentUpdateDetails.setDeleteDeltaStartTimestamp(timestamp)

          val alreadyDeletedRows: Long = rowCountDetailsVO.getDeletedRowsInBlock
          val totalDeletedRows: Long = alreadyDeletedRows + countOfRows
          segmentUpdateDetails.setDeletedRowsInBlock(totalDeletedRows.toString)
          if (totalDeletedRows == rowCountDetailsVO.getTotalNumberOfRows) {
            segmentUpdateDetails.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
          }
          else {
            // write the delta file
            carbonDeleteWriter.write(deleteDeltaBlockDetails)
          }

          deleteStatus = SegmentStatus.SUCCESS
        } catch {
          case e : MultipleMatchingException =>
            LOGGER.error(e.getMessage)
          // don't throw exception here.
          case e: Exception =>
            val errorMsg = s"Delete data operation is failed for ${ database }.${ tableName }."
            LOGGER.error(errorMsg + e.getMessage)
            throw e
        }


        var finished = false

        override def hasNext: Boolean = {
          if (!finished) {
            finished = true
            finished
          }
          else {
            !finished
          }
        }

        override def next(): (SegmentStatus, (SegmentUpdateDetails, ExecutionErrors)) = {
          finished = true
          result.getKey(deleteStatus, (segmentUpdateDetails, executorErrors))
        }
      }
      resultIter
    }

    segmentsTobeDeleted
  }

  def clearDistributedSegmentCache(carbonTable: CarbonTable,
      segmentsToBeCleared: Seq[Segment]): Unit = {
    if (CarbonProperties.getInstance().isDistributedPruningEnabled(carbonTable
      .getDatabaseName, carbonTable.getTableName)) {
      try {
        IndexServer.getClient.invalidateSegmentCache(carbonTable
          .getDatabaseName, carbonTable.getTableName, segmentsToBeCleared.map(_.getSegmentNo)
          .toArray)
      } catch {
        case _: Exception =>
          LOGGER.warn(s"Clearing of invalid segments for ${
            carbonTable.getTableUniqueName} has failed")
      }
    }
  }

  private def createCarbonInputFormat(absoluteTableIdentifier: AbsoluteTableIdentifier) :
  (CarbonTableInputFormat[Array[Object]], Job) = {
    val carbonInputFormat = new CarbonTableInputFormat[Array[Object]]()
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    val job: Job = new Job(jobConf)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    (carbonInputFormat, job)
  }
}
