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

package org.apache.carbondata.examples

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat

import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("INFO")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark2/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark2/src/main/resources/log4j.properties")

//    CarbonProperties.getInstance()
//      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
//    spark.sql("drop table if exists maintable")
//    spark.sql("CREATE TABLE mainTable(id int, name string, city string, age string) stored by 'carbondata'")
//    spark.sql("insert into mainTable values(1,'vishal', 'city1', 21)")
//    spark.sql("insert into mainTable values(2,'likun', 'city2', 42)")
//    spark.sql("insert into mainTable values(3,'ravi', 'city3', 32)")
//    spark.sql("select count(*) from maintable").show()
//    val table2columnset1 = Map("maintable" -> Set("id", "name", "city", "age"))
//    spark.sql(s"ANALYZE TABLE maintable COMPUTE STATISTICS")
//    val cols = table2columnset1.get("maintable").map(_.mkString(", ")).getOrElse("")
//    spark.sql(s"ANALYZE TABLE maintable COMPUTE STATISTICS FOR COLUMNS $cols")
//    val a = spark.sql("select name, sum(age) from maintable group by name").queryExecution
//    val stats = spark.table("default.maintable").queryExecution.analyzed.asInstanceOf[SubqueryAlias].child.asInstanceOf[LogicalRelation].stats(spark.sessionState.conf)
//    print(a)

//    val table2columnset1 = Map("maintable" -> Set("id", "name", "city", "age"))
//    spark.sql(s"ANALYZE TABLE maintable COMPUTE STATISTICS")
//    val cols = table2columnset1.get("maintable").map(_.mkString(", ")).getOrElse("")
//    spark.sql(s"ANALYZE TABLE maintable COMPUTE STATISTICS FOR COLUMNS $cols")
//    val newIdentifier = TableIdentifier("maintable", Some("default"))
//    var catalogTable = spark.sessionState.catalog.getTableMetadata(newIdentifier)
//    catalogTable.copy(stats = Some(CatalogStatistics(100, Some(200))))
//    catalogTable = catalogTable.copy(stats = Some(CatalogStatistics(100, Some(200))))
//    spark.sessionState.catalog.alterTable(catalogTable)
//    print()

    val maintable = List("fact1", "fact2", "fact3")
    val dimensionTable = List("dim1", "dim2", "dim3")


  }

  def getColStats(): (String, ColumnStat) = {
    ("age", new ColumnStat(1, Some(1), Some(2), 0, 4, 4))
  }

}