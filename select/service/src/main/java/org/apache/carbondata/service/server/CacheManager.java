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
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CacheManager {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(CacheManager.class.getName());

  private Map<Table, Long> memoryCache = new HashMap<Table, Long>();

  private Map<Table, Long> diskCache = new HashMap<Table, Long>();

  public CacheManager() {
  }

  public void cacheTableInMemory(Table table, String sourcePath, String targetPath,
      Configuration configuration) throws VisionException {
    if (memoryCache.get(table) != null) {
      return;
    }

    copyFilesToCache(sourcePath, targetPath, configuration);
    memoryCache.put(table, System.currentTimeMillis());
    LOGGER.audit("cached table in memory: " + table);
  }

  public void cacheTableToDisk(Table table, String sourcePath, String targetPath,
      Configuration configuration) throws VisionException {
    if (diskCache.get(table) != null) {
      return;
    }

    copyFilesToCache(sourcePath, targetPath, configuration);
    diskCache.put(table, System.currentTimeMillis());
    LOGGER.audit("cached table in disk: " + table);
  }

  private void copyFilesToCache(String sourcePath, String targetPath, Configuration configuration)
      throws VisionException {
    try {
      if (FileFactory.isFileExist(targetPath)) {
        boolean status = FileFactory.deleteAllFilesOfDir(new File(targetPath));
        if (!status) {
          throw new VisionException("Failed to delete the cache table: " + targetPath);
        }
      }
    } catch (IOException e) {
      String message = "Failed to check whether the cache table is exists or not";
      LOGGER.error(e);
      throw new VisionException(message);
    }

    try {
      Path source = new Path(sourcePath);
      FileSystem fs = source.getFileSystem(configuration);
      fs.copyToLocalFile(false, source, new Path(targetPath), false);
    } catch (IOException e) {
      String message =
          "Failed to copy the table to local cache store " + sourcePath + " -> " + targetPath;
      LOGGER.error(e);
      throw new VisionException(message);
    }
  }

  public Long getMemoryCache(Table table) {
    return memoryCache.get(table);
  }

  public Long getDiskCache(Table table) {
    return diskCache.get(table);
  }
}
