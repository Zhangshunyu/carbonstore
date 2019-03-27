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

import java.util

import scala.collection.JavaConverters._
import scala.reflect.classTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.DataSkewRangePartitioner
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil
import org.apache.carbondata.core.keytable.{PartitionMeta, PartitionMetaManager, RawRowComparator}
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableHandOffPostEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.merger.{CompactionResultSortProcessor, CompactionType}
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}

object TableWithPrimaryKey {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def handoffStreamingSegment(
      spark: SparkSession,
      table: CarbonTable,
      options: Map[String, String]
  ): Unit = {
    var loadStatus = SegmentStatus.SUCCESS
    var errorMessage: String = "Handoff failure"
    var model: CarbonLoadModel = null
    // 1. lock compaction
    val identifier = table.getAbsoluteTableIdentifier
    val lock = CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.HANDOFF_LOCK)
    try {
      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the handoff lock for table" +
                    s" ${ table.getDatabaseName }.${ table.getTableName }")
        val loadMetadataDetails = readTableStatus(identifier)
        if (null != loadMetadataDetails) {
          val streamLoads =
            loadMetadataDetails.filter(_.getSegmentStatus == SegmentStatus.STREAMING_FINISH)
          if (streamLoads.isEmpty) {
            // 3. if there is no streaming finish segment
            LOGGER.warn("There is no streaming finished segment in table" +
                        s" ${ table.getDatabaseName }.${ table.getTableName }")
          } else {
            // 4. partition the streaming finished segments to collect the new partition bounds
            val segments = new util.ArrayList[Segment](streamLoads.length)
            streamLoads.foreach { load =>
              segments.add(new Segment(load.getLoadName))
            }
            val hadoopConf = spark.sessionState.newHadoopConf()
            val model = generateCarbonLoadModel(table, options, hadoopConf)
            createNewColumnarSegment(model)
            val rowComparator = new RawRowComparator(table.getNumberOfSortColumns)
            object RowOrdering extends Ordering[Array[AnyRef]] {
              def compare(rowA: Array[AnyRef], rowB: Array[AnyRef]): Int = {
                rowComparator.compare(rowA, rowB)
              }
            }
            val (splitSize, maxSpitSize, boundsOfStreaming) =
              calculateBoundsOfStreaming(spark, table, segments, RowOrdering)
            // 5. collect the old partition bounds from the index files in the columnar segments
            val oldPartitions: Array[PartitionMeta] = PartitionMetaManager.readPartitionMeta(
              CarbonTablePath.getTablePartitionMetaFilePath(identifier.getTablePath))
            // 6. merge the old partition bounds and the new partition bounds into the final bounds
            val partitioner = PartitionerMerger.generatePartitioner(
              table,
              oldPartitions,
              boundsOfStreaming,
              splitSize,
              maxSpitSize,
              RowOrdering)
            // 7. create a new segment to hand off the streaming segments
            // 8. use the final bounds to partition the streaming finished segments
            // 9. sort and write data files
            val partitionMetaList = new RawStreamDataScanRDD(spark, table, segments, false)
              .map((_, null))
              .partitionBy(partitioner)
              .map(_._1)
              .mapPartitionsWithIndex { (index, iter) =>
                sortAndWriteDataFile(index, iter, model)
              }.collect()

            val finalStatus = if (partitionMetaList.isEmpty) {
              false
            } else {
              partitionMetaList.forall(_._2)
            }

            if (finalStatus) {
              // 10.1 update segment file and table status file
              updatePartitionMeta(
                identifier.getTablePath,
                oldPartitions,
                partitionMetaList.map(_._1),
                RowOrdering)
              updateSegmentFile(model)
              updateTableStatus(model, segments)

              val handOffPostEvent: AlterTableHandOffPostEvent =
                AlterTableHandOffPostEvent(
                  spark,
                  table,
                  CarbonCommonConstants.LOAD_FOLDER + model.getSegmentId)
              OperationListenerBus.getInstance
                .fireEvent(handOffPostEvent, new OperationContext)
            } else {
              // 10.2 clean temp files
              throw new CarbonDataLoadingException("handoff task failure, please check log")
            }
          }
        }
      }
    } catch {
      case ex: Exception =>
        loadStatus = SegmentStatus.LOAD_FAILURE
        LOGGER.error(s"Handoff failed on key table ${ table.getTableUniqueName }", ex)
        errorMessage = errorMessage + ": " + ex.getCause
        LOGGER.error(errorMessage)
    } finally {
      // 11. release lock
      if (null != lock) {
        lock.unlock()
      }
      if (loadStatus.equals(SegmentStatus.LOAD_FAILURE)) {
        if (model != null) {
          CarbonLoaderUtil.updateTableStatusForFailure(model)
          if (model.getSegmentId != null) {
            CarbonLoaderUtil.deleteSegment(model, model.getSegmentId.toInt)
          }
        }
      }
    }
  }

  private def generateCarbonLoadModel(
      table: CarbonTable,
      options: Map[String, String],
      hadoopConf: Configuration
  ): CarbonLoadModel = {
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    val model = new CarbonLoadModel
    new CarbonLoadModelBuilder(table).build(
      (options ++ Map("header" -> "false")).asJava,
      optionsFinal,
      model,
      hadoopConf)
    model
  }

  private def createNewColumnarSegment(model: CarbonLoadModel): Unit = {
    val newMetaEntry = new LoadMetadataDetails
    model.setFactTimeStamp(System.currentTimeMillis())
    CarbonLoaderUtil.populateNewLoadMetaEntry(
      newMetaEntry,
      SegmentStatus.INSERT_IN_PROGRESS,
      model.getFactTimeStamp,
      false)
    CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, model, true, false)
  }

  private def calculateBoundsOfStreaming(
      spark: SparkSession,
      table: CarbonTable,
      segments: util.List[Segment],
      ordering: Ordering[Array[AnyRef]]
  ): (Long, Long, Array[Array[AnyRef]]) = {
    val projection = new CarbonProjection()
    table.getPrimaryKeys.asScala.foreach { column =>
      projection.addColumn(column)
    }
    val sampleRDD = new RawStreamDataScanRDD(spark, table, segments, true, projection)
    val totalSize =
      sampleRDD
        .getPartitions
        .map(_.asInstanceOf[StreamPartition].inputSplit.getLength)
        .sum
        .toDouble
    val maxSplitSize = 1024L * 1024 * table.getBlockletSizeInMB
    val numPartitions = Math.ceil(totalSize / maxSplitSize).toInt
    val splitSize = (totalSize / numPartitions).toLong
    val rangeBounds = new DataSkewRangePartitioner(
      numPartitions, sampleRDD.map((_, null)))(ordering, classTag[Array[AnyRef]])
      .rangeBounds
    (splitSize, maxSplitSize, rangeBounds)
  }

  private def extractSortColumnsFromStartEndKey(
      table: CarbonTable,
      startKey: Array[AnyRef],
      endKey: Array[AnyRef]
  ): (Array[AnyRef], Array[AnyRef]) = {
    val noDictMapping = CarbonDataProcessorUtil.getNoDictSortColMapping(table)
    val dataTypes = CarbonDataProcessorUtil.getNoDictDataTypes(table)
    val sortColumnsOfStartKey = new Array[AnyRef](noDictMapping.length)
    var dictIndex = 0
    var noDictIndex = 0
    if (startKey != null) {
      (0 until noDictMapping.length).foreach { index =>
        if (noDictMapping(index)) {
          sortColumnsOfStartKey(index) =
            DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(
              startKey(WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX)
                .asInstanceOf[Array[AnyRef]](noDictIndex),
              dataTypes(noDictIndex))
          noDictIndex += 1
        } else {
          sortColumnsOfStartKey(index) =
            startKey(WriteStepRowUtil.DICTIONARY_DIMENSION)
              .asInstanceOf[Array[AnyRef]](dictIndex)
          dictIndex += 1
        }
      }
    }

    dictIndex = 0
    noDictIndex = 0
    val sortColumnsOfEndKey = new Array[AnyRef](noDictMapping.length)
    if (endKey != null) {
      (0 until noDictMapping.length).foreach { index =>
        if (noDictMapping(index)) {
          sortColumnsOfEndKey(index) =
            DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(
              endKey(WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX)
                .asInstanceOf[Array[AnyRef]](noDictIndex),
              dataTypes(noDictIndex))
          noDictIndex += 1
        } else {
          sortColumnsOfEndKey(index) =
            endKey(WriteStepRowUtil.DICTIONARY_DIMENSION)
              .asInstanceOf[Array[AnyRef]](dictIndex)
          dictIndex += 1
        }
      }
    }
    (sortColumnsOfStartKey, sortColumnsOfEndKey)
  }

  private def sortAndWriteDataFile(
      taskIndex: Integer,
      iter: Iterator[Array[AnyRef]],
      model: CarbonLoadModel
  ): Iterator[(PartitionMeta, Boolean)] = {
    val rawIter = new RawResultIterator(null, null, null, true) {
      override def hasNext: Boolean = iter.hasNext

      override def next(): Array[AnyRef] = iter.next()
    }
    val rawIterList = new util.ArrayList[RawResultIterator]()
    rawIterList.add(rawIter)
    model.setTaskNo("" + taskIndex)
    val processor: CompactionResultSortProcessor = prepareHandoffProcessor(model)
    val status = processor.execute(rawIterList, null)
    val (startKey, endKey) =
      extractSortColumnsFromStartEndKey(
        model.getCarbonDataLoadSchema.getCarbonTable,
        processor.getStartKey,
        processor.getEndKey)
    val partitionMeta =
      new PartitionMeta(
        taskIndex,
        startKey,
        endKey,
        processor.getTotalFileSize)
    new Iterator[(PartitionMeta, Boolean)] {
      private var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (PartitionMeta, Boolean) = {
        finished = true
        (partitionMeta, status)
      }
    }
  }

  private def prepareHandoffProcessor(
      model: CarbonLoadModel
  ): CompactionResultSortProcessor = {
    val carbonTable = model.getCarbonDataLoadSchema.getCarbonTable
    val wrapperColumnSchemaList = CarbonUtil.getColumnSchemaList(
      carbonTable.getDimensionByTableName(carbonTable.getTableName),
      carbonTable.getMeasureByTableName(carbonTable.getTableName))
    val dimLensWithComplex =
      (0 until wrapperColumnSchemaList.size()).map(_ => Integer.MAX_VALUE).toArray
    val dictionaryColumnCardinality =
      CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList)
    val segmentProperties =
      new SegmentProperties(wrapperColumnSchemaList, dictionaryColumnCardinality)

    new CompactionResultSortProcessor(
      model,
      carbonTable,
      segmentProperties,
      CompactionType.STREAMING,
      carbonTable.getTableName,
      null
    )
  }

  private def readTableStatus(
      identifier: AbsoluteTableIdentifier
  ): Array[LoadMetadataDetails] = {
    var loadMetadataDetails: Array[LoadMetadataDetails] = null
    val segmentStatusManager = new SegmentStatusManager(identifier)

    // lock table to read table status file
    val statusLock = segmentStatusManager.getTableStatusLock
    try {
      if (statusLock.lockWithRetries()) {
        // 2. list segments
        loadMetadataDetails = SegmentStatusManager.readLoadMetadata(
          CarbonTablePath.getMetadataPath(identifier.getTablePath))
      }
      loadMetadataDetails
    } finally {
      if (null != statusLock) {
        statusLock.unlock()
      }
    }
  }

  private def updateTableStatus(model: CarbonLoadModel, segments: util.ArrayList[Segment]): Unit = {
    var status = false
    val metaDataFilepath = model.getCarbonDataLoadSchema.getCarbonTable.getMetadataPath
    val identifier = model.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier
    val metadataPath = CarbonTablePath.getMetadataPath(identifier.getTablePath)
    val fileType = FileFactory.getFileType(metadataPath)
    if (!FileFactory.isFileExist(metadataPath, fileType)) {
      FileFactory.mkdirs(metadataPath, fileType)
    }
    val tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath)
    val segmentStatusManager = new SegmentStatusManager(identifier)
    val carbonLock = segmentStatusManager.getTableStatusLock
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
          "Acquired lock for table" + model.getDatabaseName() + "." + model.getTableName()
          + " for table status updating")
        val listOfLoadFolderDetailsArray =
          SegmentStatusManager.readLoadMetadata(metaDataFilepath)

        // update new columnar segment to success status
        val newSegment =
          listOfLoadFolderDetailsArray.find(_.getLoadName.equals(model.getSegmentId))
        if (newSegment.isEmpty) {
          throw new Exception("Failed to update table status for new segment")
        } else {
          newSegment.get.setSegmentStatus(SegmentStatus.SUCCESS)
          newSegment.get.setLoadEndTime(System.currentTimeMillis())
          CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(newSegment.get, model.getSegmentId,
            model.getCarbonDataLoadSchema.getCarbonTable)
        }

        // update streaming segment to compacted status
        segments.asScala.foreach { segment =>
          val streamSegment =
            listOfLoadFolderDetailsArray.find(_.getLoadName.equals(segment.getSegmentNo))
          if (streamSegment.isEmpty) {
            throw new Exception(
              s"Failed to update table status for segment " + segment.getSegmentNo)
          } else {
            streamSegment.get.setSegmentStatus(SegmentStatus.COMPACTED)
            streamSegment.get.setMergedLoadName(model.getSegmentId)
          }
        }

        // refresh table status file
        SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray)
        status = true
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + model
          .getDatabaseName() + "." + model.getTableName())
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" +
                    model.getDatabaseName() + "." + model.getTableName())
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + model.getDatabaseName() +
                     "." + model.getTableName() + " during table status updation")
      }
    }
  }

  private def updateSegmentFile(model: CarbonLoadModel): Unit = {
    val table = model.getCarbonDataLoadSchema.getCarbonTable
    val segmentFileName =
      SegmentFileStore.writeSegmentFile(table, model.getSegmentId,
        String.valueOf(model.getFactTimeStamp))

    SegmentFileStore.updateSegmentFile(
      table,
      model.getSegmentId,
      segmentFileName,
      table.getCarbonTableIdentifier.getTableId,
      new SegmentFileStore(table.getTablePath, segmentFileName))
  }

  private def updatePartitionMeta(tablePath: String,
      oldPartitionMetas: Array[PartitionMeta],
      partitionMetas: Array[PartitionMeta],
      rowOrdering: Ordering[Array[AnyRef]]
  ) = {
    var index = 0
    oldPartitionMetas.foreach { entry =>
      index = entry.getIndex
      if (partitionMetas(index).getSize == 0) {
        partitionMetas(index).setStartKey(entry.getStartKey)
        partitionMetas(index).setEndKey(entry.getEndKey)
      } else {
        // merge startKey, endKey
        if (rowOrdering.lt(entry.getStartKey, partitionMetas(index).getStartKey)) {
          partitionMetas(index).setStartKey(entry.getStartKey)
        }
        if (rowOrdering.gt(entry.getEndKey, partitionMetas(index).getEndKey)) {
          partitionMetas(index).setEndKey(entry.getEndKey)
        }
      }
      partitionMetas(index).addSize(entry.getSize)
    }

    // update index of PartitionMeta list
    val finalPartitionMetas = partitionMetas
      .filter(_.getSize > 0)
      .zipWithIndex
      .map { entry =>
        entry._1.setIndex(entry._2)
        entry._1
      }

    // refresh partitionmeta file
    PartitionMetaManager.updatePartitionMeta(
      CarbonTablePath.getTablePartitionMetaFilePath(tablePath),
      finalPartitionMetas)
  }
}
