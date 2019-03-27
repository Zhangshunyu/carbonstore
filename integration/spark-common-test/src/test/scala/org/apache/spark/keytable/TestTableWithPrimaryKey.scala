/*
 * Licensed to the Apache Software Foundation (ASF) under onassemblye
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.keytable

import java.io.PrintWriter
import java.net.{BindException, ServerSocket}

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

class TestTableWithPrimaryKey extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("generate streaming segments and hand off with RangeSort") {
    createTable()
    ingestData()

//    sql("clean files for table pktable.table1").show(100, false)
    // sql("alter table pktable.table1 COMPACT 'close_streaming'").show(100, false)

//    sql("SET carbon.input.segments.pktable.table1=6").show(100, false)

    sql("select count(*), min(id), max(id) from pktable.table1").show(100, false)
    sql("show segments for table pktable.table1").show(100, false)

     val table = CarbonEnv.getCarbonTable(Option("pktable"), "table1")(spark)
     TableWithPrimaryKey.handoffStreamingSegment(spark, table, Map.empty)

    sql("select count(*), min(id), max(id) from pktable.table1").show(100, false)
    sql("show segments for table pktable.table1").show(100, false)
  }

  def createTable(): Unit = {
    sql("DROP DATABASE IF EXISTS pktable CASCADE")
    sql("CREATE DATABASE pktable")
    sql("USE pktable")
    sql(
      s"""
         | CREATE TABLE pktable.table1(
         | id BIGINT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('streaming'='true',
         | 'primary_key_columns'='id,city',
         | 'sort_columns'='id,city',
         | 'sort_scope'='local_sort')
         | """.stripMargin)
  }

  def ingestData(): Unit = {
    executeStreamingIngest(
      tableName = "table1",
      batchNums = 100,
      rowNumsEachBatch = 100000,
      intervalOfSource = 5,
      intervalOfIngest = 10,
      continueSeconds = 550,
      generateBadRecords = false,
      badRecordAction = "force",
      handoffSize = 1024L * 1024 * 64,
      autoHandoff = false
    )
  }

  def executeStreamingIngest(
      tableName: String,
      batchNums: Int,
      rowNumsEachBatch: Int,
      intervalOfSource: Int,
      intervalOfIngest: Int,
      continueSeconds: Int,
      generateBadRecords: Boolean,
      badRecordAction: String,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean,
      badRecordsPath: String = CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("pktable"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].metaData.carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket()
      val thread1 = createWriteSocketThread(
        serverSocket = server,
        writeNums = batchNums,
        rowNums = rowNumsEachBatch,
        intervalSecond = intervalOfSource,
        badRecords = generateBadRecords)
      val thread2 = createSocketStreamingThread(
        spark = spark,
        port = server.getLocalPort,
        carbonTable = carbonTable,
        tableIdentifier = identifier,
        badRecordAction = badRecordAction,
        intervalSecond = intervalOfIngest,
        handoffSize = handoffSize,
        autoHandoff = autoHandoff,
        badRecordsPath = badRecordsPath)
      thread1.start()
      thread2.start()
      Thread.sleep(continueSeconds * 1000)
      thread2.interrupt()
      thread1.interrupt()
    } finally {
      if (null != server) {
        server.close()
      }
    }
  }

  def getServerSocket(): ServerSocket = {
    var port = 9071
    var serverSocket: ServerSocket = null
    var retry = false
    do {
      try {
        retry = false
        serverSocket = new ServerSocket(port)
      } catch {
        case ex: BindException =>
          retry = true
          port = port + 2
          if (port >= 65535) {
            throw ex
          }
      }
    } while (retry)
    serverSocket
  }

  def createWriteSocketThread(
      serverSocket: ServerSocket,
      writeNums: Int,
      rowNums: Int,
      intervalSecond: Int,
      badRecords: Boolean = false): Thread = {
    new Thread() {
      override def run(): Unit = {
        // wait for client to connection request and accept
        val clientSocket = serverSocket.accept()
        val socketWriter = new PrintWriter(clientSocket.getOutputStream())
        var index = 1L
        for (_ <- 1 to writeNums) {
          // write 5 records per iteration
          val stringBuilder = new StringBuilder()
          for (_ <- 1 to rowNums) {
            stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + "\002school_" + index + index + "\001" + index)
            index = index + 1
            stringBuilder.append("\n")
          }
          socketWriter.append(stringBuilder.toString())
          socketWriter.flush()
          Thread.sleep(1000 * intervalSecond)
        }
        socketWriter.close()
      }
    }
  }

  def createSocketStreamingThread(
      spark: SparkSession,
      port: Int,
      carbonTable: CarbonTable,
      tableIdentifier: TableIdentifier,
      badRecordAction: String = "force",
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean,
      badRecordsPath: String = CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL
  ): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", port)
            .load()

          // Write data from socket stream to carbondata file
          // repartition to simulate an empty partition when readSocketDF has only one row
          qry = readSocketDF.repartition(2).writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("bad_records_action", badRecordAction)
            .option("BAD_RECORD_PATH", badRecordsPath)
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option(CarbonStreamParser.CARBON_STREAM_PARSER,
              CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
            .option(CarbonCommonConstants.HANDOFF_SIZE, handoffSize)
            .option("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
            .option(CarbonCommonConstants.ENABLE_AUTO_HANDOFF, autoHandoff)
            .start()
          qry.awaitTermination()
        } catch {
          case ex: Throwable =>
            LOGGER.error(ex.getMessage)
            throw new Exception(ex.getMessage, ex)
        } finally {
          if (null != qry) {
            qry.stop()
          }
        }
      }
    }
  }

  override def afterAll(): Unit = {

  }

}
