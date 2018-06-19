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

package org.apache.carbondata.vision.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class VisionConfiguration implements Serializable, Writable {

  public static final String SELECT_PROJECTION = "carbon.select.projection";
  public static final String SELECT_FILTER = "carbon.select.filter";
  public static final String SELECT_SEARCH_COLUMN = "carbon.select.search.column";
  public static final String SELECT_SEARCH_VECTOR = "carbon.select.search.vector";
  public static final String SELECT_TOP_N = "carbon.select.algorithm.top.n";
  public static final String SELECT_VECTOR_SIZE = "carbon.select.algorithm.vector.size";
  public static final String SELECT_BATCH_SIZE = "carbon.select.store.batch.size";
  public static final String SELECT_ID = "carbon.select.id";

  public static final String SCHEDULE_CORE_NUM = "carbon.schedule.core.num";

  public static final String SERVER_HOST = "carbon.server.host";
  public static final String SERVER_PORT = "carbon.server.port";
  public static final String SERVER_LIST = "carbon.server.list";
  public static final String SERVER_CORE_NUM = "carbon.server.core.num";

  public static final String STORE_LOCATION = "carbon.store.location";
  public static final String STORE_DISK_CACHE = "carbon.store.disk.cache.location";
  public static final String STORE_MEMORY_CACHE = "carbon.store.memory.cache.location";

  public static final String CONFIG_HADOOP = "carbon.hadoop.configuration";

  private Map<String, Object> conf = new HashMap<>(10);

  public VisionConfiguration() {

  }

  public VisionConfiguration conf(String key, Object value) {
    conf.put(key, value);
    return this;
  }

  public void load(String file) {
    VisionUtil.loadProperties(file, this);
  }

  public void conf(VisionConfiguration vconf) {
    conf.putAll(vconf.conf);
  }

  public Object conf(String key) {
    return conf.get(key);
  }

  public String[] projection() {
    return stringArrayValue(SELECT_PROJECTION);
  }

  public int batchSize() {
    return intValue(SELECT_BATCH_SIZE);
  }

  public byte[] searchVector() {
    return byteArrayValue(SELECT_SEARCH_VECTOR);
  }

  public String searchColumn() {
    return stringValue(SELECT_SEARCH_COLUMN);
  }

  public int topN() {
    return intValue(SELECT_TOP_N);
  }

  public int vectorSize() {
    return intValue(SELECT_VECTOR_SIZE);
  }

  public String serverHost() {
    return stringValue(SERVER_HOST);
  }

  public int serverPort() {
    return intValue(SERVER_PORT);
  }

  public int serverCoreNum() {
    return intValue(SERVER_CORE_NUM);
  }

  public String serverList() {
    return stringValue(SERVER_LIST);
  }

  public String storeLocation() {
    return stringValue(STORE_LOCATION);
  }

  public String diskLocation() {
    return stringValue(STORE_DISK_CACHE);
  }

  public String memoryLocation() {
    return stringValue(STORE_MEMORY_CACHE);
  }

  public int scheduleCoreNum() {
    return intValue(SCHEDULE_CORE_NUM);
  }

  public String configHadoop() {
    return stringValue(CONFIG_HADOOP);
  }

  public String selectId() {
    return stringValue(SELECT_ID);
  }

  private String stringValue(String key) {
    Object obj = conf.get(key);
    if (obj == null) {
      return null;
    }
    return obj.toString();
  }

  private byte[] byteArrayValue(String key) {
    Object obj = conf.get(key);
    if (obj == null) {
      return null;
    }
    if (obj instanceof byte[]) {
      return (byte[]) obj;
    }
    return null;
  }

  private int intValue(String key) {
    Object obj = conf.get(key);
    if (obj == null) {
      return -1;
    }
    if (obj instanceof Integer) {
      return (Integer) obj;
    }
    if (obj instanceof String) {
      return Integer.parseInt((String) obj);
    }
    return -1;
  }

  private String[] stringArrayValue(String key) {
    Object obj = conf.get(key);
    if (obj == null) {
      return null;
    }
    if (obj instanceof String[]) {
      return (String[]) obj;
    }

    return null;
  }

  public Iterator<Map.Entry<String, Object>> iterator() {
    return conf.entrySet().iterator();
  }

  @Override public void write(DataOutput out) throws IOException {
    byte[] bytes = VisionUtil.serialize(conf);
    WritableUtils.writeCompressedByteArray(out, bytes);
  }

  @Override public void readFields(DataInput in) throws IOException {
    byte[] bytes = WritableUtils.readCompressedByteArray(in);
    conf = (Map<String, Object>) VisionUtil.deserialize(bytes);
  }
}
