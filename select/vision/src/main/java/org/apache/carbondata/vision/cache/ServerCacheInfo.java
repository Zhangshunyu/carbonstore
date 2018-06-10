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

package org.apache.carbondata.vision.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Cache server info, The cache number of different cache level
 */
public class ServerCacheInfo {
  /**
   * The index of cache level and
   * the cache number of this cache level
   */
  Map<Integer, Integer> cacheStat;

  /**
   * constructor, the default value is 0
   */
  public ServerCacheInfo() {
    cacheStat = new HashMap<Integer, Integer>();
    for (CacheLevel cacheLevel : CacheLevel.values()) {
      cacheStat.put(cacheLevel.getIndex(), 0);
    }
  }

  /**
   * increase the number of this cache level
   *
   * @param cacheLevel the index of cache level
   */
  public void increaseAndSet(int cacheLevel) {
    cacheStat.put(cacheLevel, 1 + cacheStat.get(cacheLevel));
  }

  /**
   * get the number of this cache level
   *
   * @param cacheLevel the index of cache level
   * @return the number of this cache level
   */
  public Integer getNumber(int cacheLevel) {
    return cacheStat.get(cacheLevel);
  }

  public Map<Integer, Integer> getCacheStat() {
    return cacheStat;
  }

  public void setCacheStat(Map<Integer, Integer> cacheStat) {
    this.cacheStat = cacheStat;
  }

  /**
   * merge the two ServerCacheInfo
   *
   * @param serverCacheInfo ServerCacheInfo object
   */
  public void merge(ServerCacheInfo serverCacheInfo) {
    for (int i : serverCacheInfo.cacheStat.keySet()) {
      this.cacheStat.put(i, this.cacheStat.get(i) + serverCacheInfo.cacheStat.get(i));
    }
  }
}
