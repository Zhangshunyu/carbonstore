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

package org.apache.spark.sql.hive.tpcds

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.spark.sql.internal.SQLConf

import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor


class TestCommonSubexpressionManager(sparkSession: SparkSession, sQLConf: SQLConf) extends CommonSubexpressionManager(sparkSession,
  sQLConf.copy(SQLConf.CASE_SENSITIVE -> false,
                       SQLConf.CBO_ENABLED -> true, 
                       SQLConf.buildConf("spark.mv.recommend.speedup.threshold").doubleConf.createWithDefault(0.5) -> 0.5,
                       SQLConf.buildConf("spark.mv.recommend.rowcount.threshold").doubleConf.createWithDefault(0.1) -> 0.1,
                       SQLConf.buildConf("spark.mv.recommend.frequency.threshold").doubleConf.createWithDefault(2) -> 2,
                       SQLConf.buildConf("spark.mv.tableCluster").stringConf.createWithDefault(s"""""") -> s"""{"fact":["default.maintable"]}""")) {
}

object TestQueryBatchPreprocessor extends QueryBatchPreprocessor