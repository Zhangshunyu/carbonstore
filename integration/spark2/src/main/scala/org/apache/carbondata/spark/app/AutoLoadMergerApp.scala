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

package org.apache.carbondata.spark.app

import java.io.File
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.command.CompactionModel
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.sdk.file.{CarbonSchemaReader, CarbonWriter}
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, StreamHandoffRDD}
import org.apache.carbondata.spark.util.CarbonSparkUtil


object AutoLoadMergerApp {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

 def main(args: Array[String]): Unit = {

   var spark: CarbonSession = null
   var model: CarbonLoadModel = null
   while (true) {
     try {

       val storePath = if (args.length > 0) args.head else throw new UnsupportedOperationException(
         "tablepath must be present")
       var detailses = SegmentStatusManager
         .readLoadMetadata(CarbonTablePath.getMetadataPath(storePath))
       val streamSegments = detailses.filter(_.getSegmentStatus == SegmentStatus.STREAMING_FINISH)
       if (streamSegments.length > 0) {
         if (spark == null) {
           spark = createCarbonSession(args, storePath)
           ThreadLocalSessionInfo
             .setConfigurationToCurrentThread(spark.sparkContext.hadoopConfiguration)
         }
         if (model == null) {
           model = getModel(storePath)
         }
         StreamHandoffRDD.startStreamingHandoffThread(
           model,
           new OperationContext,
           spark, true)
       }
       if (model == null) {
         model = getModel(storePath)
       }
       val compactionType = CompactionType.MINOR
       detailses = SegmentStatusManager
         .readLoadMetadata(CarbonTablePath.getMetadataPath(storePath))
       val compactionSize = CarbonDataMergerUtil.getCompactionSize(compactionType, model)
       val loadsToMerge = CarbonDataMergerUtil
         .identifySegmentsToBeMerged(model,
           compactionSize,
           detailses.toList.asJava,
           compactionType,
           new util.ArrayList[String]())
       var compactSegmentsSize = 3
       if (args.length > 1) {
         compactSegmentsSize = args(1).toInt
       }
       if (loadsToMerge.size() > compactSegmentsSize) {
         if (spark == null) {
           spark = createCarbonSession(args, storePath)
           ThreadLocalSessionInfo
             .setConfigurationToCurrentThread(spark.sparkContext.hadoopConfiguration)
         }
         val compactionModel = CompactionModel(compactionSize,
           compactionType,
           model.getCarbonDataLoadSchema.getCarbonTable,
           true,
           None,
           None
         )

         // normal flow of compaction
         val lock = CarbonLockFactory.getCarbonLockObj(
           AbsoluteTableIdentifier.from(storePath),
           LockUsage.COMPACTION_LOCK)
         if (lock.lockWithRetries()) {
           LOGGER.info("Acquired the compaction lock for table with path: " + storePath)
           try {
             CarbonDataRDDFactory.startCompactionThreads(
               spark.sqlContext,
               model,
               storePath,
               compactionModel,
               lock,
               new util.ArrayList[String](),
               new OperationContext
             )
           } catch {
             case e: Exception =>
               LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
               lock.unlock()
               throw e
           }
         } else {
           LOGGER.error(s"Not able to acquire the compaction lock for table" + storePath)
           CarbonException.analysisException(
             "Table is already locked for compaction. Please try after some time.")
         }
       }
     } catch {
       case e: Exception => e.printStackTrace()
     }
     Thread.sleep(5000)
   }
   if (spark != null) {
     println("Shutting down ...")
     spark.stop()
   }
 }

  private def getModel(storePath: String) = {
    val table = CarbonTable
      .buildFromTablePath(String.valueOf(System.nanoTime()),
        "default",
        storePath,
        String.valueOf(System.nanoTime()))
    new CarbonLoadModelBuilder(table)
      .build(table.getTableInfo.getFactTable.getTableProperties,
        System.nanoTime(),
        String.valueOf(System.currentTimeMillis()))
  }

  private def createCarbonSession(args: Array[String],
      storePath: String): CarbonSession = {
    import org.apache.spark.sql.CarbonSession._
    val sparkConf = new SparkConf(loadDefaults = true)

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    if (args.length != 0 && args.length != 1 && args.length != 4) {
      logger.error("parameters: storePath [access-key] [secret-key] [s3-endpoint]")
      System.exit(0)
    }

    val builder = SparkSession
      .builder().master("local")
      .config(sparkConf)
      .appName("AutoLoadMergerApp(uses CarbonSession)")
      .enableHiveSupport()

    if (!sparkConf.contains("carbon.properties.filepath")) {
      val sparkHome = System.getenv.get("SPARK_HOME")
      if (null != sparkHome) {
        val file = new File(sparkHome + '/' + "conf" + '/' + "carbon.properties")
        if (file.exists()) {
          builder.config("carbon.properties.filepath", file.getCanonicalPath)
          System.setProperty("carbon.properties.filepath", file.getCanonicalPath)
        }
      }
    } else {
      System.setProperty("carbon.properties.filepath", sparkConf.get("carbon.properties.filepath"))
    }
    val spark = if (args.length <= 1) {
      builder.getOrCreateCarbonSession(storePath)
    } else {
      val (accessKey, secretKey, endpoint) = CarbonSparkUtil.getKeyOnPrefix(args(0))
      builder.config(accessKey, args(1))
        .config(secretKey, args(2))
        .config(endpoint, CarbonSparkUtil.getS3EndPoint(args))
        .getOrCreateCarbonSession(storePath)
    }
    SparkSession.setActiveSession(spark)
    spark.asInstanceOf[CarbonSession]
  }
}
