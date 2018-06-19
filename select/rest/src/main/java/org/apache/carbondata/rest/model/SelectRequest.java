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

package org.apache.carbondata.rest.model;

import org.apache.carbondata.rest.model.deserialize.VisionConfigurationDeserializer;
import org.apache.carbondata.rest.model.serialize.VisionConfigurationSerializer;
import org.apache.carbondata.vision.common.VisionConfiguration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class SelectRequest {

  private String tableName;
  private String[] projection;
  private String filter;
  private int limit;

  @JsonSerialize(using = VisionConfigurationSerializer.class)
  @JsonDeserialize(using = VisionConfigurationDeserializer.class)
  private VisionConfiguration configuration;

  public SelectRequest() {

  }

  public SelectRequest(String tableName) {
    this.tableName = tableName;
  }

  public SelectRequest(String tableName, String[] projection, String filter, int limit,
      VisionConfiguration configuration) {
    this.tableName = tableName;
    this.projection = projection;
    this.filter = filter;
    this.limit = limit;
    this.configuration = configuration;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getProjection() {
    return projection;
  }

  public void setProjection(String[] projection) {
    this.projection = projection;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public VisionConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(VisionConfiguration configuration) {
    this.configuration = configuration;
  }
}
