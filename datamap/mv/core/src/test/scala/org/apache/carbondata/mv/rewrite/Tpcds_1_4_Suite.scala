/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import scala.util.control.NonFatal

import org.apache.carbondata.mv.testutil.ModularPlanTest
import org.apache.spark.sql.catalyst.util._
import org.apache.carbondata.mv.dsl.plans._
import org.apache.carbondata.mv.session.MVSession
import org.apache.spark.sql.hive.test.TestHive
import org.apache.carbondata.mv.testutil.Tpcds_1_4_QueryBatch
import org.apache.carbondata.mv.testutil.Tpcds_1_4_Tables
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.SQLBuilder
import org.apache.spark.sql.AnalysisException
import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.catalyst.expressions._
import java.io.{BufferedReader, File, FileInputStream, IOException, InputStreamReader, PrintWriter}
import org.apache.spark.internal.Logging

class Tpcds_1_4_Suite extends ModularPlanTest with BeforeAndAfter { 
  import org.apache.spark.sql.hive.test.TestHive.implicits._
  import org.apache.carbondata.mv.testutil.Tpcds_1_4_Tables._
  import org.apache.carbondata.mv.rewrite.matching.TestTPCDS_1_4_Batch._

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate() //config("spark.sql.warehouse.dir","/home/ey-chih/workspace/carbonstore/datamap/mv/core/spark-warehouse").getOrCreate()
  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
  val hiveClient = testHive.sparkSession.metadataHive

  test("test using tpc-ds queries") {

    tpcds1_4Tables.foreach { create_table =>
      hiveClient.runSqlHive(create_table)
    }

    val writer = new PrintWriter(new File("./output/batch.txt"))
//    val dest = "case_30"
//    val dest = "case_32"
//    val dest = "case_33"
// case_15 and case_16 need revisit

    val dest = "case_39"   /** to run single case, uncomment out this **/
    
    tpcds_1_4_testCases.foreach { testcase =>
      if (testcase._1 == dest) { /** to run single case, uncomment out this **/
        val mvSession = new MVSession(testHive.sparkSession) 
        val summaryDF = testHive.sparkSession.sql(testcase._2)
        mvSession.sharedState.registerSummaryDataset(summaryDF)

        writer.print(s"\n\n==== ${testcase._1} ====\n\n==== mv ====\n\n${testcase._2}\n\n==== original query ====\n\n${testcase._3}\n")
        
        val rewriteSQL = mvSession.rewriteToSQL(mvSession.sparkSession.sql(testcase._3).queryExecution.analyzed)
        logInfo(s"\n\n\n\n===== Rewritten query for ${testcase._1} =====\n\n${rewriteSQL}\n")
        
        if (!rewriteSQL.trim.equals(testcase._4)) {
          logError(s"===== Rewrite not matched for ${testcase._1}\n")
          logError(s"\n\n===== Rewrite failed for ${testcase._1}, Expected: =====\n\n${testcase._4}\n")
          logError(
              s"""
              |=== FAIL: SQLs do not match ===
              |${sideBySide(rewriteSQL, testcase._4).mkString("\n")}
              """.stripMargin)
          writer.print(s"\n\n==== result ====\n\nfailed\n")
          writer.print(s"\n\n==== rewritten query ====\n\n${rewriteSQL}\n")
        }
        else {
          logInfo(s"===== Rewrite successful for ${testcase._1}, as expected\n")
          writer.print(s"\n\n==== result ====\n\nsuccessful\n")
          writer.print(s"\n\n==== rewritten query ====\n\n${rewriteSQL}\n")
        }

        }  /**to run single case, uncomment out this **/
    
    }

    writer.close()
  }
}