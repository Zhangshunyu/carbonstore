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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.service.common.TableCacheInfo;
import org.apache.carbondata.service.master.CarbonMaster;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.service.service.PredictServiceFactory;
import org.apache.carbondata.vision.cache.ServerCacheInfo;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.mortbay.log.Log;

public class CarbonScheduler {

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
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      List<PredictService> services = new ArrayList<>(coreNum);
      for (int i = 0; i < coreNum; i++) {
        services.add(PredictServiceFactory.getPredictService(serverInfo));
      }
      serviceMap.put(serverInfo, new ServicePool(services));
    }
  }

  public boolean loadLibrary(String libName) {
    boolean status = true;
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      status = status && serviceMap.get(serverInfo).get().loadLibrary(libName);
    }
    return status;
  }

  public Model loadModel(String modelPath) throws VisionException {
    Model model = null;
    for (ServerInfo serverInfo: CarbonMaster.serverList()) {
      model = serviceMap.get(serverInfo).get().loadModel(modelPath);
    }
    return model;
  }

  public byte[] cacheTable(Table table, int cacheLevel, boolean allNode, Set<Table> tableSet)
      throws VisionException {
    List<ServerInfo> serverInfoList = CarbonMaster.serverList();
    int serverCount = serverInfoList.size();
    TableCacheInfo tableCacheInfo = CarbonMaster.getCacheInfo(table);
    if (tableCacheInfo == null) {
      tableCacheInfo = new TableCacheInfo(serverInfoList);
    }
    if (allNode) {
      for (int i = 0; i < serverCount; i++) {
        tableCacheInfo.add(serverInfoList.get(i), cacheLevel);
      }
    } else {
      ServerInfo chooseServer = chooseCacheServer(tableSet, cacheLevel);
      tableCacheInfo.add(chooseServer, cacheLevel);
    }
    byte[] serializedTable = null;
    try {
      Iterator<ServerInfo> itor = tableCacheInfo.getServerInfoSet().iterator();
      while (itor.hasNext()) {
        serializedTable = serviceMap.get(itor.next()).get().cacheTable(table, cacheLevel);
      }
      return serializedTable;
    } finally {
      if (serializedTable != null) {
        CarbonMaster.addCacheInfo(table, tableCacheInfo);
      }
    }
  }

  /**
   * Choose server for caching
   *
   * @param tableSet   table set
   * @param cacheLevel cache level
   * @return ServerInfo object of cache server
   */
  public ServerInfo chooseCacheServer(Set<Table> tableSet, int cacheLevel) {
    ServerInfo chooseServerInfo = null;
    Iterator<Table> itor = tableSet.iterator();
    Map<ServerInfo, ServerCacheInfo> map = new HashMap<>();
    while (itor.hasNext()) {
      Table table = itor.next();
      TableCacheInfo tableCacheInfo = CarbonMaster.getCacheInfo(table);
      Iterator<ServerInfo> serverInfoIterator = tableCacheInfo.getServerInfoSet().iterator();
      while (serverInfoIterator.hasNext()) {
        ServerInfo serverInfo = serverInfoIterator.next();
        ServerCacheInfo serverCacheInfo = tableCacheInfo.getServerCacheInfo(serverInfo);
        ServerCacheInfo serverCacheInfoAll = map.get(serverInfo);
        if (serverCacheInfoAll == null) {
          map.put(serverInfo, serverCacheInfo);
        } else {
          serverCacheInfoAll.merge(serverCacheInfo);
          map.put(serverInfo, serverCacheInfoAll);
        }
      }
    }
    int min = Integer.MAX_VALUE;

    for (Map.Entry<ServerInfo, ServerCacheInfo> entry : map.entrySet()) {
      int number = entry.getValue().getNumber(cacheLevel);
      if (number < min) {
        min = number;
        chooseServerInfo = entry.getKey();
      }
    }
    Log.info("Choose Cache Server:" +
            chooseServerInfo + "\t The min value is " + min + "\t");
    return chooseServerInfo;
  }

  /**
   * Choose server for searching
   *
   * @param serverInfos Table cache info
   * @return ServerInfo of search server
   */
  public ServerInfo chooseSearchServer(TableCacheInfo serverInfos) {
    Iterator<ServerInfo> itor = serverInfos.getServerInfoSet().iterator();
    ServerInfo chooseServerInfo = null;
    int max = Integer.MIN_VALUE;
    while (itor.hasNext()) {
      ServerInfo serverInfo = itor.next();
      int value = serverInfo.getCores() - Integer.parseInt(serverInfo.getWorkload().toString());
      Log.info("Search server status:" + serverInfo + "\tThe available value is " + value);
      if (max < value) {
        max = value;
        chooseServerInfo = serverInfo;
      }
    }
    return chooseServerInfo;
  }

  public Record[] search(CarbonMultiBlockSplit split, PredictContext context)
      throws VisionException {
    ServerInfo chooseServer = chooseSearchServer(CarbonMaster.getCacheInfo(context.getTable()));
    Log.info("Choose search server is " + chooseServer);
    if (chooseServer == null) {
      int serverCount = CarbonMaster.serverList().size();
      int serverIndex = (int) (cacheCount.getAndIncrement() % serverCount);
      chooseServer = CarbonMaster.serverList().get(serverIndex);
    }
    chooseServer.getWorkload().incrementAndGet();
    Record[] records = null;
    try {
      records = serviceMap.get(chooseServer).get().search(split, context);
    } catch (Exception e) {
      throw e;
    } finally {
      chooseServer.getWorkload().decrementAndGet();
    }
    return records;
  }

}
