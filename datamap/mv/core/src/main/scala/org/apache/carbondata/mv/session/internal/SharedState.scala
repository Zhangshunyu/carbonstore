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

package org.apache.carbondata.mv.session.internal

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.{SQLListener, SQLTab}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.spark.sql.DataFrame
import org.apache.carbondata.mv.rewrite.SummaryDatasetCatalog
import java.io.File
import scala.io.Source

/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 */
private[mv] class SharedState(
    @transient val sparkContext: SparkContext,
    @transient private val existingCatalog: Option[SummaryDatasetCatalog]) {
  
  def this(sparkContext: SparkContext) = {
    this(sparkContext, None)
  }

  @transient
  lazy val summaryDatasetCatalog = {
    existingCatalog.getOrElse(new SummaryDatasetCatalog(new SQLContext(sparkContext).sparkSession))
  }
  
//  def clearSummaryDatasetCatalog(): Unit = summaryDatasetCatalog.clearSummaryDatasetCatalog()
//  
  def registerSummaryDataset(query: DataFrame): Unit = summaryDatasetCatalog.registerSummaryDataset(query)
//  
  def unregisterSummaryDataset(query: DataFrame): Unit = summaryDatasetCatalog.unregisterSummaryDataset(query)
//  
//  def refreshSummaryDatasetCatalog(filePath: String): Unit = {
//    val file = new File(filePath)
//    val sqlContext = new SQLContext(sparkContext)
//    if (file.exists) {
//      clearSummaryDatasetCatalog()
//      for (line <- Source.fromFile(file).getLines) {
//        registerSummaryDataset(sqlContext.sql(line))
//      }
//    }
//  }

  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager
}

object SharedState {}

