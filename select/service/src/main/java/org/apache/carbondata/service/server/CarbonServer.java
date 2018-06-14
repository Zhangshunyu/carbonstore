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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.service.scan.CarbonScan;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.service.service.impl.PredictServiceImpl;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public class CarbonServer {

  private static LogService LOGGER = LogServiceFactory.getLogService(CarbonServer.class.getName());

  private Map<Table, byte[]> serializedTables = new HashMap<>();

  private Map<Table, CarbonTable> tables = new HashMap<>();

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
    cacheManager = new CacheManager();
  }

  public byte[] getTable(Table table) throws VisionException {
    byte[] serializedTable = serializedTables.get(table);
    if (serializedTable != null) {
      return serializedTable;
    }

    String tablePath = getTableFolder(table);
    try {
      org.apache.carbondata.format.TableInfo tableInfo =
          CarbonUtil.readSchemaFile(CarbonTablePath.getSchemaFilePath(tablePath));
      SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
      TableInfo tableInfo1 =
          schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "", "", "");
      tableInfo1.setTablePath(tablePath);
      return tableInfo1.serialize();
    } catch (IOException e) {
      String message = "Failed to get table from " + tablePath;
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  public String getTableFolder(Table table) {
    return conf.storeLocation() + File.separator + table.getDatabase() + File.separator +
        table.getTableName();
  }

  public String getMemoryFolder(Table table) {
    return conf.memoryLocation() + File.separator + table.getDatabase() + File.separator +
        table.getTableName();
  }

  public String getDiskFolder(Table table) {
    return conf.diskLocation() + File.separator + table.getDatabase() + File.separator +
        table.getTableName();
  }

  public CarbonTable cacheMeta(Table table) throws VisionException {
    CarbonTable carbonTable = tables.get(table);
    if (carbonTable != null) {
      return carbonTable;
    }

    try {
      byte[] bytes = getTable(table);
      carbonTable = CarbonTable.buildFromTableInfo(TableInfo.deserialize(bytes));
      tables.put(table, carbonTable);
      return carbonTable;
    } catch (IOException e) {
      String message = "Failed to cache meta: " + table.getPresentName();
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  public void cacheData(Table table, CacheLevel level) throws VisionException {
    if (CacheLevel.Memory == level) {
      cacheManager.cacheTableInMemory(table, getTableFolder(table), getMemoryFolder(table),
          FileFactory.getConfiguration());
    } else if (CacheLevel.Disk == level) {
      cacheManager.cacheTableToDisk(table, getTableFolder(table), getDiskFolder(table),
          FileFactory.getConfiguration());
    }
  }

  // update file path to cache path
  private CarbonMultiBlockSplit useCacheTable(Table table, CarbonMultiBlockSplit multiBlockSplit,
      String selectId) {
    boolean useMemory = cacheManager.getMemoryCache(table) != null;
    boolean useDisk = cacheManager.getDiskCache(table) != null;

    if (useMemory || useDisk) {
      List<CarbonInputSplit> splits = multiBlockSplit.getAllSplits();
      List<InputSplit> cachedSplits = new ArrayList<>(splits.size());
      try {
        for (CarbonInputSplit split : splits) {
          String fileName = CarbonTablePath.DataFileUtil.getFileName(split.getPath().toString());
          String cacheTablePath = null;
          if (useMemory) {
            cacheTablePath = getMemoryFolder(table);
          } else if (useDisk) {
            cacheTablePath = getDiskFolder(table);
          }
          String segmentPath =
              CarbonTablePath.getSegmentPath(cacheTablePath, split.getSegmentId());
          String cacheFilePath = segmentPath + File.separator + fileName;
          CarbonInputSplit cachedSplit =
              new CarbonInputSplit(split.getSegmentId(), split.getBlockletId(),
                  new Path(cacheFilePath), split.getStart(), split.getLength(),
                  split.getLocations(), split.getVersion(), split.getDeleteDeltaFiles(),
                  split.getDataMapWritePath());
          cachedSplit.setDetailInfo(split.getDetailInfo());
          cachedSplits.add(cachedSplit);
        }
      } catch (IOException e) {
        LOGGER.error(e);
      }
      LOGGER.audit("[" + selectId + "] table " + table.getPresentName() + " is using " +
          (useMemory ? "memory" : "disk") + "cache");
      return new CarbonMultiBlockSplit(cachedSplits);
    }

    LOGGER.audit("[" + selectId + "] table " + table.getPresentName() + " doesn't hit cache");
    return multiBlockSplit;
  }

  public Record[] search(CarbonMultiBlockSplit split, PredictContext context)
      throws VisionException {
    long startTime = System.currentTimeMillis();
    String selectId = context.getConf().selectId();
    RecordReader<Void, Object> reader = null;
    try {
      CarbonTable carbonTable = tables.get(context.getTable());
      if (carbonTable == null) {
        LOGGER.audit("[" + selectId + "] CarbonServer cache meta during search.");
        carbonTable = cacheMeta(context.getTable());
      }
      CarbonMultiBlockSplit cachedSplit = useCacheTable(context.getTable(), split, selectId);
      reader = CarbonScan.createRecordReader(cachedSplit, context, carbonTable);
      List<Object> result = new ArrayList<>();
      while (reader.nextKeyValue()) {
        result.add(reader.getCurrentValue());
      }
      Record[] records = new Record[result.size()];
      for (int i = 0; i < records.length; i++) {
        records[i] = new Record((Object[]) result.get(i));
      }
      return records;
    } catch (Exception e) {
      String message = "[" + selectId + "] Failed to search feature on table: " +
          context.getTable().getPresentName();
      LOGGER.error(e, message);
      throw new VisionException(message);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOGGER.audit("[" + selectId + "] Failed to close RecordReader");
        }
      }
      long endTime = System.currentTimeMillis();
      LOGGER.audit("[" + selectId + "] CarbonServer search table: " +
          context.getTable().getPresentName() + " ,taken time: " + (endTime - startTime) + " ms");
    }
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
