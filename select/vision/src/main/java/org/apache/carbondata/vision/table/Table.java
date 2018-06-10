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

package org.apache.carbondata.vision.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Table implements Serializable, Writable {

  private String database;
  private String tableName;
  private String featureVectorName;

  public Table() {
  }

  public Table(String tableName) {
    this.tableName = tableName;
    this.database = "default";
    this.featureVectorName = "feature";
  }

  public Table(String database, String tableName, String featureVectorName) {
    this.database = database;
    this.tableName = tableName;
    this.featureVectorName = featureVectorName;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getFeatureVectorName() {
    return featureVectorName;
  }

  public void setFeatureVectorName(String featureVectorName) {
    this.featureVectorName = featureVectorName;
  }

  @Override public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, database);
    WritableUtils.writeString(out, tableName);
    WritableUtils.writeString(out, featureVectorName);
  }

  @Override public void readFields(DataInput in) throws IOException {
    database = WritableUtils.readString(in);
    tableName = WritableUtils.readString(in);
    featureVectorName = WritableUtils.readString(in);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Table table = (Table) o;
    return Objects.equals(database, table.database) && Objects.equals(tableName, table.tableName);
  }

  @Override public int hashCode() {

    return Objects.hash(database, tableName);
  }

  public String getPresentName() {
    return database + "." + tableName;
  }

  @Override public String toString() {
    return "Table{" + "database='" + database + '\'' + ", tableName='" + tableName + '\'' + '}';
  }
}
