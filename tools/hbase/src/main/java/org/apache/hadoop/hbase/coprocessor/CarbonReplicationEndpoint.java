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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.sdk.file.CarbonSchemaWriter;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private public class CarbonReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(CarbonReplicationEndpoint.class);

  private Configuration conf;
  // Size limit for replication RPCs, in bytes
  private int replicationRpcLimit;

  // Thread pool executor to write the table into carbon file format
  private ThreadPoolExecutor exec;
  private int maxThreads;

  // HBase table descriptors
  private TableDescriptors tableDescriptors;

  // Map of table and pair of table schema and properties
  private Map<TableName, CarbonHbaseMeta> tableSchemaMap = Maps.newConcurrentMap();

  // Map of regions and carbon writer
  private Map<String, CarbonWriter> regionsWriterMap = Maps.newConcurrentMap();

  @Override public UUID getPeerUUID() {
    return ctx.getClusterId();
  }

  @Override public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override public void start() {
    startAsync();
  }

  @Override public void stop() {
    closeCarbonWriters();
    stopAsync();
  }

  @Override protected void doStart() {
    notifyStarted();
  }

  @Override protected void doStop() {
    notifyStopped();
  }

  @Override public void init(Context context) throws IOException {
    super.init(context);
    conf = ctx.getConfiguration();
    tableDescriptors = ctx.getTableDescriptors();
    this.replicationRpcLimit =
        (int) (0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE, RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    // Initialize the executor
    this.maxThreads = conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
        HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    this.exec = new ThreadPoolExecutor(maxThreads, maxThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.exec.allowCoreThreadTimeOut(true);
  }

  @Override public boolean replicate(ReplicateContext replicateContext) {
    CompletionService<Integer> pool = new ExecutorCompletionService<>(this.exec);
    try {
      // parse the replication entries to region wise
      List<List<Entry>> batches = createBatches(replicateContext.getEntries());
      while (this.isRunning() && !exec.isShutdown()) {

        if (!isPeerEnabled()) {
          Threads.sleep(1000);
          continue;
        } else {
          break;
        }
      }
      // TODO: Move this logic, can be done while writing to the file
      // Initialize and cache the carbon writer for each region
      initCarbonWriters(batches);
      // Replicate the entries concurrently based on batches
      parallelReplicate(pool, replicateContext, batches);
    } catch (Exception e) {
      LOG.error("Exception occured while writing the data", e);
      return false;
    }

    return true;
  }

  /**
   * Divide the entries into multiple batches, so that we can replicate each batch in a thread pool
   * concurrently. Note that, for serial replication, we need to make sure that entries from the
   * same region to be replicated serially, so entries from the same region consist of a batch, and
   * we will divide a batch into several batches by replicationRpcLimit in method
   * serialReplicateRegionEntries()
   */
  private List<List<Entry>> createBatches(final List<Entry> entries) {
    Map<byte[], List<Entry>> regionEntries = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry entry : entries) {
      // Skip those table entries where CARBON_SCHEMA doesn't exist in table descriptor
      TableName tableName = entry.getKey().getTableName();
      try {
        if (tableDescriptors.get(tableName).getValue(CarbonMasterObserver.CARBON_SCHEMA_DESC)
            == null) {
          continue;
        }
      } catch (IOException io) {
        LOG.error("Exception occured while retrieving carbon schema details of table=" + tableName
            .getNameAsString(), io);
        continue;
      }

      regionEntries.computeIfAbsent(entry.getKey().getEncodedRegionName(), key -> new ArrayList<>())
          .add(entry);
    }
    return new ArrayList<>(regionEntries.values());
  }

  private long parallelReplicate(CompletionService<Integer> pool, ReplicateContext replicateContext,
      List<List<Entry>> batches) throws IOException {
    int futures = 0;
    for (int i = 0; i < batches.size(); i++) {
      List<Entry> entries = batches.get(i);
      if (!entries.isEmpty()) {
        LOG.trace("Submitting {} entries of total size {}", entries.size(),
            replicateContext.getSize());
        // RuntimeExceptions encountered here bubble up and are handled in ReplicationSource
        pool.submit(createReplicator(entries, i));
        futures++;
      }
    }

    IOException iox = null;
    long lastWriteTime = 0;
    for (int i = 0; i < futures; i++) {
      try {
        // wait for all futures, remove successful parts
        // (only the remaining parts will be retried)
        Future<Integer> f = pool.take();
        int index = f.get();
        List<Entry> batch = batches.get(index);
        batches.set(index, Collections.emptyList()); // remove successful batch
        // Find the most recent write time in the batch
        long writeTime = batch.get(batch.size() - 1).getKey().getWriteTime();
        if (writeTime > lastWriteTime) {
          lastWriteTime = writeTime;
        }
      } catch (InterruptedException ie) {
        iox = new IOException(ie);
      } catch (ExecutionException ee) {
        // cause must be an IOException
        iox = (IOException) ee.getCause();
      }
    }
    if (iox != null) {
      // if we had any exceptions, try again
      throw iox;
    }
    return lastWriteTime;
  }

  protected Callable<Integer> createReplicator(List<Entry> entries, int batchIndex) {
    return () -> serialReplicateRegionEntries(entries, batchIndex);
  }

  private int serialReplicateRegionEntries(List<Entry> entries, int batchIndex) throws IOException {
    int batchSize = 0, index = 0;
    List<Entry> batch = new ArrayList<>();
    for (Entry entry : entries) {
      int entrySize = getEstimatedEntrySize(entry);
      if (batchSize > 0 && batchSize + entrySize > replicationRpcLimit) {
        writeToCarbonFile(batch, index++);
        batch.clear();
        batchSize = 0;
      }
      batch.add(entry);
      batchSize += entrySize;
    }
    if (batchSize > 0) {
      writeToCarbonFile(batch, index);
    }
    return batchIndex;
  }

  /*
   * Returns approximate entry size
   */
  private int getEstimatedEntrySize(Entry e) {
    long size = e.getKey().estimatedSerializedSizeOf() + e.getEdit().estimatedSerializedSizeOf();
    return (int) size;
  }

  /*
   * Whether peer is enabled
   */
  protected boolean isPeerEnabled() {
    return ctx.getReplicationPeer().isPeerEnabled();
  }

  private void initCarbonWriters(List<List<Entry>> batches) {
    for (List<Entry> entry : batches) {
      if (entry.isEmpty()) {
        continue;
      }

      for (Entry walEntry : entry) {
        // Create carbon writer for each region with the specific path
        String regionName = Bytes.toString(walEntry.getKey().getEncodedRegionName());
        if (regionsWriterMap.get(regionName) == null) {
          try {
            TableName tableName = walEntry.getKey().getTableName();
            createCarbonWriter(tableName, regionName);
          } catch (Exception e) {
            LOG.error("Exception occured while initializing carbon writer for region " + regionName,
                e);
          }
        }
      }
    }
  }

  private void createCarbonWriter(TableName tableName, String regionName)
      throws IOException, InvalidLoadOptionException {

    Schema tableSchema = null;
    Map<String, String> tblproperties = null;
    CarbonHbaseMeta hbaseMeta = tableSchemaMap.get(tableName);
    // Create carbon writer and cache it
    if (hbaseMeta == null) {
      // Get the schema from table desc and convert it into JSON format
      String schemaDesc =
          tableDescriptors.get(tableName).getValue(CarbonMasterObserver.CARBON_SCHEMA_DESC);
      tblproperties = new HashMap<>();
      tableSchema = CarbonSchemaWriter.convertToSchemaFromJSON(schemaDesc, tblproperties);
      hbaseMeta = new CarbonHbaseMeta(tableSchema, tblproperties);
      tableSchemaMap.put(tableName, hbaseMeta);
    } else {
      tableSchema = hbaseMeta.getSchema();
      tblproperties = hbaseMeta.getTblProperties();
    }
    String path = tblproperties.get(CarbonMasterObserver.PATH);
    Map<String, String> clonedProps = new HashMap<>(tblproperties);
    clonedProps.remove(CarbonMasterObserver.HBASE_MAPPING_DETAILS);
    clonedProps.remove(CarbonMasterObserver.PATH);

    CarbonWriterBuilder builder =
        CarbonWriter.builder().outputPath(path).withTableProperties(clonedProps)
            .withRowFormat(tableSchema).writtenBy(CarbonReplicationEndpoint.class.getSimpleName())
            .withHadoopConf(conf);
    regionsWriterMap.put(regionName, builder.build());
  }

  private void closeCarbonWriters() {
    try {
      for (CarbonWriter writer : regionsWriterMap.values()) {
        writer.close();
      }
    } catch (Exception e) {
      LOG.error("Exception occured while closing the carbon writer", e);
    }
  }

  // Table schema details
  //
  // {
  // �ID�:"INT",
  // �name�:"string",
  // �department�:"string",
  // �salary�:"double"
  // �tblproperties�: {�sort_columns�:"ID",
  // �hbase_mapping�:"key=ID,cf1.name=name,cf1.dept=department,cf2.sal=salary",
  // �path�:"dlc://user.bucket1/customer"}
  // }
  private void writeToCarbonFile(List<Entry> batch, int index) {
    String regionName = Bytes.toString(batch.get(0).getKey().getEncodedRegionName());
    try {
      // Check and create the write if not exist
      TableName tName = batch.get(index).getKey().getTableName();
      CarbonWriter carbonWriter = regionsWriterMap.get(regionName);
      if (carbonWriter == null) {
        createCarbonWriter(tName, regionName);
        carbonWriter = regionsWriterMap.get(regionName);
      }
      CarbonHbaseMeta hbaseMeta = tableSchemaMap.get(tName);
      // Check the keys in sequence order and if the keys are same then merge to the same carbon row.
      ByteArrayWrapper rowKey = null;
      String[] row = null;
      for (Entry entry : batch) {
        for (Cell cell : entry.getEdit().getCells()) {
          ByteArrayWrapper tmpKey =
              new ByteArrayWrapper(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          boolean merge = false;
          if (rowKey != null) {
            if (rowKey.equals(tmpKey)) {
              merge = true;
            }
          }

          if (merge) {
            fillCell(row, cell, hbaseMeta);
          } else {
            if (row != null) {
              carbonWriter.write(row);
            }
            row = new String[hbaseMeta.getSchema().getFieldsLength()];
            int keyColumnIndex = hbaseMeta.getKeyColumnIndex();
            row[keyColumnIndex] = hbaseMeta
                .convertData(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                    keyColumnIndex);
            row[hbaseMeta.getTimestampMapIndex()] = String.valueOf(cell.getTimestamp());
            fillCell(row, cell, hbaseMeta);
          }
          rowKey = tmpKey;
        }
      }
      if (row != null) {
        carbonWriter.write(row);
      }
      carbonWriter.flushBatch();
    } catch (Exception e) {
      LOG.error("Exception occured while performing writer flush", e);
    }
  }

  private static void fillCell(String[] row, Cell cell, CarbonHbaseMeta meta) {
    int index = meta.getSchemaIndexOfColumn(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    String data =
        meta.convertData(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), index);
    row[index] = data;
  }
}
