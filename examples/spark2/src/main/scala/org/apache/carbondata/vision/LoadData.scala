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

package org.apache.carbondata.vision

import java.io._
import java.util.Random

import org.apache.commons.codec.binary.Hex

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object LoadData {

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath

  val carbon = ExampleUtils.createCarbonSession("FRS", 1)

  def main(args: Array[String]): Unit = {
    val createTable = true
    val tableName = "frs20_table"
    if (createTable) {
      val filePath = rootPath + s"/examples/spark2/src/main/resources/${ tableName }.csv"
      generateData(rootPath + "/select/build/carbonselect/test/result_20w.bin",
        filePath,
        200000,
        1)

      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "128")

      carbon.sql(s"drop table if exists default.${ tableName }")
      carbon
        .sql(
          s"create table default.${ tableName }(id int, feature binary) stored by " +
          s"'carbondata' " +
          "tblproperties('TABLE_BLOCKSIZE'='512')")
      carbon
        .sql(s"load data local inpath '${ filePath }' into table default.${ tableName } options" +
             s"('header'='false')")
    }
    carbon.sql(s"select count(id) from default.${ tableName } limit 10").show(false)
    carbon.sql(s"select * from default.${ tableName } where id in (100, 100000, 800000)")
      .show(false)

    carbon.sql(s"select * from default.${ tableName } limit 10").show(false)
  }

  val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray
  val random = new Random()


  def generateString(builder: java.lang.StringBuilder): Unit = {
    (0 until 10).foreach { x =>
      builder.append(alphabet(random.nextInt(alphabet.length)))
    }
  }

  def generateData(binFilePath: String, filePath: String, length: Int, loop: Int): Unit = {
    var reader: BufferedInputStream = null
    var fileWriter: BufferedWriter = null
    try {

      fileWriter = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(filePath), "UTF-8"))
      val bytes = new Array[Byte](288)
      var tmp = -1
      val builder = new java.lang.StringBuilder(320)
      var index = 0
      (1 to loop).foreach { _ =>
        reader = new BufferedInputStream(new FileInputStream(binFilePath))
        (1 to length).foreach { _ =>
          index = index + 1
          tmp = reader.read(bytes)
          if (tmp != -1) {
            builder.setLength(0)
            if (index > 1) {
              builder.append("\n")
            }
            builder.append(index)
              .append(",")
            // generateString(builder)
            // builder.append(",")
              .append(Hex.encodeHex(bytes))
            fileWriter.write(builder.toString)
          }
          if (index % 1000 == 0) {
            fileWriter.flush()
          }
        }
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case _ =>
          }
        }
      }
    } finally {

      if (fileWriter != null) {
        try {
          fileWriter.close()
        } catch {
          case _ =>
        }
      }
    }
  }

  def generateData2(binFilePath: String, filePath: String, length: Int, loop: Int): Unit = {
    var fileWriter: BufferedWriter = null
    try {
      val random = new Random()
      fileWriter = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(filePath), "UTF-8"))
      val bytes = new Array[Byte](288)
      val builder = new java.lang.StringBuilder(320)
      var index = 0
      (1 to loop).foreach { _ =>
        (1 to length).foreach { _ =>
          index = index + 1
          random.nextBytes(bytes)
          builder.setLength(0)
          if (index > 1) {
            builder.append("\n")
          }
          builder
            .append(index)
            .append(",")
          generateString(builder)
          builder
            .append(",")
            .append(Hex.encodeHex(bytes))
          fileWriter.write(builder.toString)
          if (index % 10000 == 0) {
            fileWriter.flush()
          }
        }
      }
    } finally {
      if (fileWriter != null) {
        try {
          fileWriter.close()
        } catch {
          case _ =>
        }
      }
    }
  }


}
