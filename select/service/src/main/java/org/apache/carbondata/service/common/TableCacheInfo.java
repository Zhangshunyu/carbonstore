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

package org.apache.carbondata.service.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.vision.cache.ServerCacheInfo;

/**
 * table cache info, including server info and server cache info
 */
public class TableCacheInfo {
  /**
   * The  info of all table cache  server
   */
  private Map<ServerInfo, ServerCacheInfo> serverCacheMap;

  public TableCacheInfo() {
    this.serverCacheMap = new HashMap<>();
  }

  public TableCacheInfo(List<ServerInfo> serverInfoSet) {
    serverCacheMap = new HashMap<>();
    Iterator<ServerInfo> itor = serverInfoSet.iterator();
    while (itor.hasNext()) {
      serverCacheMap.put(itor.next(), new ServerCacheInfo());
    }
  }

  public TableCacheInfo(Set<ServerInfo> serverInfoSet) {
    serverCacheMap = new HashMap<>();
    Iterator<ServerInfo> itor = serverInfoSet.iterator();
    while (itor.hasNext()) {
      serverCacheMap.put(itor.next(), new ServerCacheInfo());
    }
  }

  /**
   * Add server info and change the number of cache level
   *
   * @param serverInfo Server Info
   * @param cacheLevel the index of cache level
   */
  public void add(ServerInfo serverInfo, Integer cacheLevel) {
    ServerCacheInfo serverCacheInfo = new ServerCacheInfo();
    serverCacheInfo.increaseAndSet(cacheLevel);
    serverCacheMap.put(serverInfo, serverCacheInfo);
  }

  /**
   * Add server info set
   *
   * @param serverInfoSet
   */
  //TODO: considerate the cache level
  public void addAll(Set<ServerInfo> serverInfoSet) {
    serverInfoSet.addAll(serverInfoSet);
  }

  /**
   * get all info of this table cache server
   *
   * @return server info set
   */
  public Set<ServerInfo> getServerInfoSet() {
    return serverCacheMap.keySet();
  }

  public Map<ServerInfo, ServerCacheInfo> getServerCacheMap() {
    return serverCacheMap;
  }

  public void setServerCacheMap(Map<ServerInfo, ServerCacheInfo> serverCacheMap) {
    this.serverCacheMap = serverCacheMap;
  }

  /**
   * get server cache info by server info
   *
   * @param serverInfo server info
   * @return server cache info
   */
  public ServerCacheInfo getServerCacheInfo(ServerInfo serverInfo) {
    return this.serverCacheMap.get(serverInfo);
  }
}
