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

package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution

/**
 * A dataframe that wraps a given row array, overrides all dataframe's action
 */
private[sql] class UnlazyDataFrame(
    spark: SparkSession,
    lp: LogicalPlan,
    rows: Array[Row])
  extends Dataset[Row](spark, new QueryExecution(spark, lp), RowEncoder(lp.schema)) {

  override def collect(): Array[Row] = rows

  override def take(n: Int): Array[Row] = rows.take(n)

  override def count(): Long = rows.length

  override def head(n: Int): Array[Row] = take(n)

  override def reduce(func: (Row, Row) => Row): Row = rows.reduce(func)

  override def foreach(f: Row => Unit): Unit = rows.foreach(f)

  override def foreachPartition(f: Iterator[Row] => Unit): Unit = f.apply(rows.iterator)

  override def toLocalIterator(): java.util.Iterator[Row] = rows.iterator.asJava

  override def toDF(): DataFrame = new UnlazyDataFrame(sparkSession, lp, rows)
}
