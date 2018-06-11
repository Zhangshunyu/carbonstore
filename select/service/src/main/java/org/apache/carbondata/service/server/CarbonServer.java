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

package org.apache.carbondata.service.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.service.service.impl.PredictServiceImpl;
import org.apache.carbondata.store.LocalCarbonStore;
import org.apache.carbondata.store.MetaCachedCarbonStore;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.InputSplit;

public class CarbonServer {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonServer.class.getName());

  private MetaCachedCarbonStore store;

  private VisionConfiguration conf;

  private CacheManager cacheManager;

  // vm argument: -Djava.library.path=
  // /home/david/Documents/code/carbonstore/select/vision-native/build/lib
  // program args:
  // /home/david/Documents/code/carbonstore/select/build/carbonselect/conf/server/log4j.properties
  // .../carbonstore/select/build/carbonselect/conf/server/carbonselect.properties
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: CarbonServer <log4j file> <properties file>");
      return;
    }
    Utils.initLog4j(args[0]);
    VisionConfiguration conf = new VisionConfiguration();
    conf.load(args[1]);
    new CarbonServer(conf).start();
  }

  public CarbonServer(VisionConfiguration conf) {
    this.conf = conf;
    store = new LocalCarbonStore();
    cacheManager = new CacheManager();
  }

  public CarbonTable getTable(Table table) throws VisionException {
    String tablePath = getTableFolder(table);
    try {
      return store.getTable(tablePath);
    } catch (IOException e) {
      String message = "Failed to get table from CarbonServer store";
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  public String getTableFolder(Table table) {
    return conf.storeLocation() + File.separator + table.getDatabase() + File.separator +
        table.getTableName();
  }

  public String getCacheFolder(Table table) {
    return conf.cacheLocation() + File.separator + table.getDatabase() + File.separator +
        table.getTableName();
  }

  public void cacheTable(Table table, CacheLevel level) throws VisionException {
    if (CacheLevel.Memory == level) {
      cacheManager.cacheTableInMemory(table);
    } else if (CacheLevel.Disk == level) {
      cacheManager.cacheTableToDisk(table, getTableFolder(table), getCacheFolder(table),
          FileFactory.getConfiguration());
    }
  }

  public CarbonMultiBlockSplit useCacheTable(Table table,
      CarbonMultiBlockSplit multiBlockSplit) {
    if (cacheManager.getMemoryCache(table) != null) {
      // TODO
      return multiBlockSplit;
    } else if (cacheManager.getDiskCache(table) != null) {
      List<CarbonInputSplit> splits = multiBlockSplit.getAllSplits();
      List<InputSplit> cachedSplits = new ArrayList<>(splits.size());
      try {
        for (CarbonInputSplit split: splits) {
          String fileName =
              CarbonTablePath.DataFileUtil.getFileName(split.getPath().toString());
          String cacheTablePath = getCacheFolder(table);
          String segmentPath =
              CarbonTablePath.getSegmentPath(cacheTablePath, split.getSegmentId());
          String cacheFilePath = segmentPath + File.separator + fileName;
          CarbonInputSplit cachedSplit =
              new CarbonInputSplit(
                  split.getSegmentId(),
                  split.getBlockletId(),
                  new Path(cacheFilePath),
                  split.getStart(),
                  split.getLength(),
                  split.getLocations(),
                  split.getVersion(),
                  split.getDeleteDeltaFiles(),
                  split.getDataMapWritePath());
          cachedSplit.setDetailInfo(split.getDetailInfo());
          cachedSplits.add(cachedSplit);
        }
      } catch (IOException e) {
        LOGGER.error(e);
      }
      return new CarbonMultiBlockSplit(cachedSplits);
    }
    return multiBlockSplit;
  }

  public void start() throws IOException {
    Configuration hadoopConf = FileFactory.getConfiguration();
    hadoopConf.addResource(new Path(conf.configHadoop()));
    RPC.Builder builder = new RPC.Builder(hadoopConf);
    RPC.Server server =
        builder.setNumHandlers(conf.serverCoreNum()).setBindAddress(conf.serverHost())
            .setPort(conf.serverPort()).setProtocol(PredictService.class)
            .setInstance(new PredictServiceImpl(this)).build();
    server.start();
  }
}
