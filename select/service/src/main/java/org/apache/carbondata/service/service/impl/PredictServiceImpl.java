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

package org.apache.carbondata.service.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.service.scan.CarbonScan;
import org.apache.carbondata.service.server.CarbonServer;
import org.apache.carbondata.service.service.PredictService;
import org.apache.carbondata.vision.cache.CacheLevel;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.library.LibraryManager;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.model.ModelManager;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.RecordReader;

public class PredictServiceImpl implements PredictService {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(PredictServiceImpl.class.getName());

  private CarbonServer server;

  public PredictServiceImpl(CarbonServer server) {
    this.server = server;
  }

  @Override public boolean loadLibrary(String libName) {
    return LibraryManager.loadLibrary(libName);
  }

  @Override public Model loadModel(String modelPath) throws VisionException {
    return ModelManager.loadModel(modelPath);
  }

  @Override public byte[] cacheTable(Table table, int cacheLevel) throws VisionException {
    try {
      byte[] bytes = server.getTable(table).getTableInfo().serialize();
      // cache table data by cacheLevel
      server.cacheTable(table, CacheLevel.get(cacheLevel));
      return bytes;
    } catch (IOException e) {
      String message = "Failed to serialize TableInfo";
      LOGGER.error(e, message);
      throw new VisionException(message);
    }
  }

  @Override public Record[] search(CarbonMultiBlockSplit split, PredictContext context)
      throws VisionException {
    long startTime = System.currentTimeMillis();
    try {
      CarbonTable carbonTable = server.getTable(context.getTable());
      CarbonMultiBlockSplit cachedSplit = server.useCacheTable(context.getTable(), split);
      RecordReader<Void, Object> reader =
          CarbonScan.createRecordReader(cachedSplit, context, carbonTable);
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
      String message = "Failed to search feature";
      LOGGER.error(e, message);
      throw new VisionException(message);
    } finally {
      long endTime = System.currentTimeMillis();
      LOGGER.audit("search taken time: " + (endTime - startTime) + " ms");
    }
  }

  @Override public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID;
  }

  @Override public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return null;
  }
}
