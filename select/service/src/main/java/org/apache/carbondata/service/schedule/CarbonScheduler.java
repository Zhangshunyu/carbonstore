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

package org.apache.carbondata.service.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.service.master.CarbonMaster;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.service.service.PredictServiceFactory;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.apache.commons.collections.CollectionUtils;

public class CarbonScheduler {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(CarbonScheduler.class.getName());

  private VisionConfiguration conf;

  private Map<ServerInfo, ServicePool> serviceMap;

  private AtomicLong cacheCount;

  private int coreNum;

  public CarbonScheduler(VisionConfiguration conf) {
    this.conf = conf;
    serviceMap = new HashMap<ServerInfo, ServicePool>();
    cacheCount = new AtomicLong(0);
    this.coreNum = 1;
  }

  public void init() throws VisionException {
    if (CarbonMaster.serverList().isEmpty()) {
      throw new VisionException("There is no server. Please check the configuration");
    }
    coreNum = conf.scheduleCoreNum();
    for (ServerInfo serverInfo : CarbonMaster.serverList()) {
      List<PredictService> services = new ArrayList<>(coreNum);
      for (int i = 0; i < coreNum; i++) {
        services.add(PredictServiceFactory.getPredictService(serverInfo));
      }
      serviceMap.put(serverInfo, new ServicePool(services));
    }
  }

  public boolean loadLibrary(String libName) {
    boolean status = true;
    for (ServerInfo serverInfo : CarbonMaster.serverList()) {
      status = status && serviceMap.get(serverInfo).get().loadLibrary(libName);
    }
    return status;
  }

  public Model loadModel(String modelPath) throws VisionException {
    Model model = null;
    for (ServerInfo serverInfo : CarbonMaster.serverList()) {
      model = serviceMap.get(serverInfo).get().loadModel(modelPath);
    }
    return model;
  }

  public byte[] getTable(Table table) throws VisionException {
    return serviceMap.get(nextServer()).get().getTable(table);
  }

  public void cacheTable(Table table, CacheLevel cacheLevel) throws VisionException {
    List<ServerInfo> serverList = CarbonMaster.serverList();
    Set<ServerInfo> cachedServers = CarbonMaster.getCacheInfo(table, cacheLevel);
    Collection<ServerInfo> candidates = serverList;
    if (cachedServers != null) {
      candidates = CollectionUtils.intersection(serverList, cachedServers);
    }
    if (!candidates.isEmpty()) {
      ServerInfo server = chooseCacheServer(candidates, cacheLevel);
      serviceMap.get(server).get().cacheTable(table, cacheLevel.getIndex());
      CarbonMaster.addCacheInfo(table, server, cacheLevel);
    }
  }

  public ServerInfo nextServer() {
    int serverCount = CarbonMaster.serverList().size();
    int serverIndex = (int) (cacheCount.getAndIncrement() % serverCount);
    return CarbonMaster.serverList().get(serverIndex);
  }

  /**
   * Choose server for caching
   *
   * @param servers    server set
   * @param cacheLevel cache level
   * @return ServerInfo object of cache server
   */
  public ServerInfo chooseCacheServer(Collection<ServerInfo> servers, CacheLevel cacheLevel) {
    ServerInfo chooseServerInfo = null;
    int min = Integer.MAX_VALUE;
    for (ServerInfo serverInfo : servers) {
      int number = serverInfo.cacheCount(cacheLevel);
      if (number < min) {
        min = number;
        chooseServerInfo = serverInfo;
      }
    }
    LOGGER.info("Choose Cache Server:" + chooseServerInfo + "\t The min value is " + min + "\t");
    return chooseServerInfo;
  }

  /**
   * Choose server for searching
   *
   * @param table Table
   * @return ServerInfo of search server
   */
  public ServerInfo chooseSearchServer(Table table) {
    Set<ServerInfo> memoryCache = CarbonMaster.getCacheInfo(table, CacheLevel.Memory);

    ServerInfo chooseServerInfo = null;
    int max = 0;
    if (memoryCache != null && memoryCache.size() > 0) {
      for (ServerInfo serverInfo : memoryCache) {
        int value = serverInfo.getCores() - serverInfo.workload();
        if (max < value) {
          max = value;
          chooseServerInfo = serverInfo;
        }
      }
    }

    if (chooseServerInfo == null) {
      max = 0;
      Set<ServerInfo> diskCache = CarbonMaster.getCacheInfo(table, CacheLevel.Disk);
      for (ServerInfo serverInfo : diskCache) {
        int value = serverInfo.getCores() - serverInfo.workload();
        if (max < value) {
          max = value;
          chooseServerInfo = serverInfo;
        }
      }
    }

    return chooseServerInfo;
  }

  public Record[] search(CarbonMultiBlockSplit split, PredictContext context)
      throws VisionException {
    ServerInfo chooseServer = chooseSearchServer(context.getTable());
    if (chooseServer == null) {
      chooseServer = nextServer();
    }
    LOGGER.audit("[" + context.getConf().selectId() + "] Choose search server is: " +
        chooseServer.getHost() + ":" + chooseServer.getPort());
    try {
      chooseServer.incrementWorkload();
      return serviceMap.get(chooseServer).get().search(split, context);
    } finally {
      chooseServer.decrementWorkload();
    }
  }

}
