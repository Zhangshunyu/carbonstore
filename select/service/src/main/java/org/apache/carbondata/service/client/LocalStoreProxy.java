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
  private Model model;
  private Algorithm algorithm;

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
    LocalStoreProxy proxy = new LocalStoreProxy(args[0], args[1], args[3]);
    // create PredictContext
    byte[] searchFeature = Utils.generateFeatureSetExample(args[2], 1, 0);
    proxy.cacheTable("table0");
    VisionConfiguration configuration = new VisionConfiguration();
    configuration.conf(VisionConfiguration.SELECT_SEARCH_VECTOR, searchFeature);
    Record[] result =
        proxy.select("table0", new String[] {"id"}, null,  10, configuration);
    Utils.printRecords(result);
    proxy.test(new Table("table0"),10, searchFeature);
  }

  public LocalStoreProxy(String log4jPropertyFilePath, String modelFilePath,
      String propertyFilePath) throws VisionException, IOException {

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

  }

  private CarbonClient createClient(String filePath) throws VisionException {
    VisionConfiguration conf = new VisionConfiguration();
    conf.load(filePath);
    CarbonClient client = new CarbonClient(conf);
    client.init();
    return client;
  }

  public void cacheTable(String tableName) throws VisionException {
    Table table = new Table(tableName);
    client.cacheTable(table);
  }

  public Record[] select(String tableName, String[] projection, String filter, int limit,
      VisionConfiguration configuration)
      throws VisionException {
    PredictContext context = PredictContext
        .builder()
        .algorithm(algorithm)
        .model(model)
        .table(new Table(tableName))
        .conf(VisionConfiguration.SELECT_TOP_N, limit)
        .conf(VisionConfiguration.SELECT_PROJECTION, projection)
        .conf(configuration)
        .create();
    return client.search(context);
  }

  // for testing purpose only
  private void test(Table table, int numThreads, byte[] searchFeature)
      throws InterruptedException, ExecutionException {
    PredictContext context = PredictContext
        .builder()
        .algorithm(algorithm)
        .model(model)
        .table(table)
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

    private static final long serialVersionUID = 1L;

    transient CarbonClient client;
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
