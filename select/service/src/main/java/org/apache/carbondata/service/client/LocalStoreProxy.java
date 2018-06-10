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

package org.apache.carbondata.service.client;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.vision.algorithm.Algorithm;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

public class LocalStoreProxy {

  private CarbonClient client;
  private Table[] tables;
  private Model model;
  private Algorithm algorithm;
  private byte[] searchFeature;

  // program args:
  // /home/david/Documents/code/carbonstore/select/build/carbonselect/conf/client/log4j.properties
  // /home/david/Documents/code/carbonstore/select/vision-native/thirdlib/intellifData
  // /home/david/Documents/code/carbonstore/examples/spark2/src/main/resources/result.bin
  // .../carbonselect/conf/client/carbonselect.properties
  public static void main(String[] args)
      throws VisionException, IOException, InterruptedException, ExecutionException {
    if (args.length != 4) {
      System.err.println(
          "Usage: LocalStoreProxy <log4j> <model path> <result.bin> <properties file>");
      return;
    }
    LocalStoreProxy proxy = new LocalStoreProxy(args[0], args[1], args[2], args[3]);
    proxy.cacheTables(3);
    Record[] result = proxy.select("table0", proxy.searchFeature);
    Utils.printRecords(result);
    proxy.test(10);
  }

  public LocalStoreProxy(String log4jPropertyFilePath, String modelFilePath,
      String featureSetFilePath, String propertyFilePath) throws VisionException, IOException {

    Utils.initLog4j(log4jPropertyFilePath);

    // start client
    client = createClient(propertyFilePath);

    // load library
    boolean isSuccess = client.loadLibrary("carbonvision");
    if (!isSuccess) {
      throw new VisionException("Failed to load library");
    }

    // load model
    model = client.loadModel(modelFilePath);

    // choose algorithm
    algorithm =
        new Algorithm("org.apache.carbondata.vision.algorithm.impl.KNNSearch", "1.0");

    // create PredictContext
    searchFeature = Utils.generateFeatureSetExample(featureSetFilePath, 1, 0);
  }

  private CarbonClient createClient(String filePath) throws VisionException {
    VisionConfiguration conf = new VisionConfiguration();
    conf.load(filePath);
    CarbonClient client = new CarbonClient(conf);
    client.init();
    return client;
  }

  public void cacheTables(int numTables) throws VisionException {
    if (numTables <= 0) {
      throw new IllegalArgumentException("numTables should be greater than zero");
    }
    tables = new Table[numTables];
    for (int i = 0; i < numTables; i++) {
      Table table = new Table("table" + i);
      CarbonTable carbonTable = client.cacheTable(table, true);
      if (carbonTable == null) {
        throw new VisionException("can not found the table: " + table.getPresentName());
      }
      tables[i] = table;
    }
  }

  public Record[] select(String tableName, byte[] searchFeature) throws VisionException {
    PredictContext context = PredictContext
        .builder()
        .algorithm(algorithm)
        .model(model)
        .table(new Table(tableName))
        .conf(VisionConfiguration.SELECT_SEARCH_VECTOR, searchFeature)
        .conf(VisionConfiguration.SELECT_TOP_N, 10)
        .conf(VisionConfiguration.SELECT_VECTOR_SIZE, 288)
        .conf(VisionConfiguration.SELECT_PROJECTION, new String[] { "id" })
        .conf(VisionConfiguration.SELECT_BATCH_SIZE, 100000)
        .create();
    return client.search(context);
  }

  // for testing purpose only
  private void test(int numThreads) throws InterruptedException, ExecutionException {
    PredictContext context = PredictContext
        .builder()
        .algorithm(algorithm)
        .model(model)
        .table(tables[0])
        .conf(VisionConfiguration.SELECT_SEARCH_VECTOR, searchFeature)
        .conf(VisionConfiguration.SELECT_TOP_N, 10)
        .conf(VisionConfiguration.SELECT_VECTOR_SIZE, 288)
        .conf(VisionConfiguration.SELECT_PROJECTION, new String[] { "id" })
        .conf(VisionConfiguration.SELECT_BATCH_SIZE, 100000)
        .create();

    ArrayList<Callable<Record[]>> tasks = new ArrayList<Callable<Record[]>>();

    for (int i = 0; i < numThreads; i++) {
      tasks.add(new QueryTask(client, context));
    }
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future<Record[]>> results = executorService.invokeAll(tasks);
    executorService.shutdown();
    executorService.awaitTermination(600, TimeUnit.SECONDS);
    for (Future<Record[]> result : results) {
      System.out.println("result length is:" + result.get().length);
    }
  }

  static class QueryTask implements Callable<Record[]>, Serializable {
    CarbonClient client;
    PredictContext context;

    public QueryTask(CarbonClient client, PredictContext context) {
      this.client = client;
      this.context = context;
    }

    @Override
    public Record[] call() throws Exception {
      Long startTime = System.nanoTime();
      Record[] result = client.search(context);
      Long endTime = System.nanoTime();
      System.out.println("search time:" + (endTime - startTime) / 1000000.0 + "ms");
      return result;
    }
  }

}
