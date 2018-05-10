/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.rewrite

import org.apache.carbondata.mv.testutil.ModularPlanTest
import org.apache.spark.sql.hive.test.TestHive
import org.apache.carbondata.mv.session.MVSession
import org.apache.spark.sql.catalyst.util._
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.SparkSession

class TestSQLSuite extends ModularPlanTest with BeforeAndAfter { 
  import org.apache.carbondata.mv.rewrite.matching.TestSQLBatch._

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
  val hiveClient = testHive.sparkSession.metadataHive
  
  test("protypical mqo rewrite test") {
    
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE Fact (
           |  `tid`     int,
           |  `fpgid`   int,
           |  `flid`    int,
           |  `date`    timestamp,
           |  `faid`    int,
           |  `price`   double,
           |  `qty`     int,
           |  `disc`    string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE       
        """.stripMargin.trim
        )
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE Dim (
           |  `lid`     int,
           |  `city`    string,
           |  `state`   string,
           |  `country` string
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE   
        """.stripMargin.trim
        )    
        
    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE Item (
           |  `i_item_id`     int,
           |  `i_item_sk`     int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE   
        """.stripMargin.trim
        )

    val dest = "case_11"
        
    sampleTestCases.foreach { testcase =>
      if (testcase._1 == dest) {
        val mvSession = new MVSession(testHive.sparkSession) 
        val summary = testHive.sparkSession.sql(testcase._2)
        mvSession.sharedState.registerSummaryDataset(summary)
        val rewrittenSQL = mvSession.rewrite(mvSession.sparkSession.sql(testcase._3).queryExecution.analyzed).toCompactSQL.trim

        if (!rewrittenSQL.trim.equals(testcase._4)) {
          fail(
              s"""
              |=== FAIL: SQLs do not match ===
              |${sideBySide(rewrittenSQL, testcase._4).mkString("\n")}
              """.stripMargin)
              }
        }
    
    }
  }

}