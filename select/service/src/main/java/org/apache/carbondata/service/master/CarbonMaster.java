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

package org.apache.carbondata.service.master;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.service.common.ServerInfo;
import org.apache.carbondata.service.common.ServiceUtil;
import org.apache.carbondata.store.CarbonRowReadSupport;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.common.VisionUtil;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

public class CarbonMaster {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonMaster.class.getName());

  private static final List<ServerInfo> serverList = new LinkedList<>();

  private static final Map<Table, Set<ServerInfo>> memoryCacheMap = new ConcurrentHashMap<>();

  private static final Map<Table, Set<ServerInfo>> diskCacheMap = new ConcurrentHashMap<>();

  public static void init(VisionConfiguration conf) {
    FileFactory.getConfiguration().addResource(new Path(conf.configHadoop()));
    ServiceUtil.parserServerList(conf.serverList(), serverList);
  }

  public static List<InputSplit> getSplit(CarbonTable table, Expression filter, String selectId)
      throws VisionException {
    long t1 = 0, t2 = 0;
    try {
      t1 = System.currentTimeMillis();
      final CarbonTableInputFormat format = new CarbonTableInputFormat();
      format.setCarbonTable(table);
      final Job job = new Job(FileFactory.getConfiguration());
      CarbonInputFormat.setTablePath(job.getConfiguration(), table.getTablePath());
      CarbonInputFormat.setTableName(job.getConfiguration(), table.getTableName());
      CarbonInputFormat.setDatabaseName(job.getConfiguration(), table.getDatabaseName());
      CarbonInputFormat.setCarbonReadSupport(job.getConfiguration(), CarbonRowReadSupport.class);

      if (filter != null) {
        CarbonInputFormat.setFilterPredicates(job.getConfiguration(), filter);
      }
      t2 = System.currentTimeMillis();
      return format.getSplits(new JobContextImpl(job.getConfiguration(), new JobID()));
    } catch (IOException e) {
      String message = "[" + selectId + "] Failed to getSplit";
      LOGGER.error(e, message);
      throw new VisionException(message);
    } finally {
      long t3 = System.currentTimeMillis();
      LOGGER.audit(
          "[" + selectId + "] CarbonMaster.getSplit " + VisionUtil.printlnTime(t1, t2, t3));
    }
  }

  public static List<ServerInfo> serverList() {
    return serverList;
  }

  public static synchronized void addCacheInfo(Table table, ServerInfo serverInfo,
      CacheLevel cacheLevel) {
    serverInfo.incrementCacheCount(cacheLevel);
    Set<ServerInfo> serverInfos;
    switch (cacheLevel) {
      case Memory:
        serverInfos =  memoryCacheMap.get(table);
        if (serverInfos == null) {
          serverInfos = new HashSet<>(serverList.size());
          memoryCacheMap.put(table, serverInfos);
        }
        serverInfos.add(serverInfo);
        break;
      case Disk:
        serverInfos =  diskCacheMap.get(table);
        if (serverInfos == null) {
          serverInfos = new HashSet<>(serverList.size());
          diskCacheMap.put(table, serverInfos);
        }
        serverInfos.add(serverInfo);
        break;
      default:
    }
  }

  public static Set<ServerInfo> getCacheInfo(Table table, CacheLevel cacheLevel) {
    Set<ServerInfo> serverInfos = null;
    switch (cacheLevel) {
      case Memory:
        serverInfos =  memoryCacheMap.get(table);
        break;
      case Disk:
        serverInfos =  diskCacheMap.get(table);
        break;
      default:
    }
    return serverInfos;
  }
}
