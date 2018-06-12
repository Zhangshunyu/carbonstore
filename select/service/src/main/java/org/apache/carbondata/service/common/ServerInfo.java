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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.vision.cache.CacheLevel;

public class ServerInfo {

  private String host;
  private int port;
  private int cores;
  private AtomicInteger workload;
  private AtomicInteger memoryCache;
  private AtomicInteger diskCache;

  public ServerInfo(String host, int port) {
    this.host = host;
    this.port = port;
    this.workload = new AtomicInteger(0);
    this.memoryCache = new AtomicInteger(0);
    this.diskCache = new AtomicInteger(0);
  }

  public ServerInfo(String host, int port, int cores) {
    this(host, port);
    this.cores = cores;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getCores() {
    return cores;
  }

  public void setCores(int cores) {
    this.cores = cores;
  }

  public void incrementCacheCount(CacheLevel cacheLevel) {
    switch (cacheLevel) {
      case Memory:
        memoryCache.incrementAndGet();
        break;
      case Disk:
        diskCache.incrementAndGet();
        break;
      default:
    }
  }

  public int cacheCount(CacheLevel cacheLevel) {
    switch (cacheLevel) {
      case Memory:
        return memoryCache.intValue();
      case Disk:
        return diskCache.intValue();
      default:
        return 0;
    }
  }

  public void incrementWorkload() {
    workload.incrementAndGet();
  }

  public void decrementWorkload() {
    workload.decrementAndGet();
  }

  public int workload() {
    return workload.intValue();
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerInfo that = (ServerInfo) o;
    return port == that.port && Objects.equals(host, that.host);
  }

  @Override public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return "ServerInfo{" +
            "host='" + host + '\'' +
            ", port=" + port +
            ", cores=" + cores +
            ", workload=" + workload +
            '}';
  }
}
