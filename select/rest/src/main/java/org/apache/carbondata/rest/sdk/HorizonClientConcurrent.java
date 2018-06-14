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

package org.apache.carbondata.rest.sdk;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.vision.table.Record;

import org.springframework.web.client.RestTemplate;

public class HorizonClientConcurrent {

  private static LogService LOGGER = LogServiceFactory
      .getLogService(HorizonClientConcurrent.class.getName());

  private String serviceUri;

  // program arguments:
  // uri: "http://localhost:8080"
  // table name: frs_table
  // /home/david/Documents/code/carbonstore/select/build/carbonselect/test/result.bin
  // for test only
  public static void main(String[] args) throws IOException,
      InterruptedException, ExecutionException {
    if (args.length < 3) {
      System.err.println("HorizonClient <rest uri> <table name> <result.bin> " +
          "[numThreads] [restAppNum] [requestNum] [cache] [tableNum]");
      return;
    }
    HorizonClientConcurrent client = new HorizonClientConcurrent(args[0]);
    String tableNamePrefix = args[1];

    int numThreads = 8;
    int restAppNum = 20;
    int requestNum = 10;
    int tableNum = 2;
    int randomLength = 10000;
    Boolean cache = true;

    if (args.length > 3) {
      numThreads = Integer.parseInt(args[3]);
    }
    if (args.length > 4) {
      restAppNum = Integer.parseInt(args[4]);
    }
    if (args.length > 5) {
      requestNum = Integer.parseInt(args[5]);
    }
    if (args.length > 6) {
      cache = Boolean.parseBoolean(args[6]);
    }
    if (args.length > 7) {
      tableNum = Integer.parseInt(args[7]);
    }

    ArrayList<Callable<Results>> tasks = new ArrayList<Callable<Results>>();
    RestApplication[] restApplications = new RestApplication[restAppNum];
    if (cache) {
      for (int i = 0; i < tableNum; i++) {
        String tableName = tableNamePrefix + i;
        LOGGER.info("Cache table " + tableName);
        client.cache(tableName);
      }
    }
    for (int i = 0; i < restAppNum; i++) {
      LOGGER.info("Init " + (i + 1) + " rest application: ");
      String tableName = tableNamePrefix + i % tableNum;
      Random random = new Random();

      RestTemplate restTemplate = new RestTemplate();
      for (int j = 0; j < requestNum; j++) {
        byte[] searchFeature = Utils
            .generateFeatureSetExample(args[2], 1, random.nextInt(randomLength));
        SelectRequest request = new SelectRequest(tableName, searchFeature);
        restApplications[i] = client.createRestApp(restTemplate, request);
        tasks.add(new QueryTask(restApplications[i]));
      }
    }
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future<Results>> results = null;
    LOGGER.info("Starting invoke tasks:");
    long allStartTime = System.nanoTime();
    results = executorService.invokeAll(tasks);
    executorService.shutdown();
    executorService.awaitTermination(600, TimeUnit.SECONDS);

    long allEndTime = System.nanoTime();
    LOGGER.info("Finish all tasks:");
    LOGGER.info("Time: " + (allEndTime - allStartTime) / 1000000.0 + " ms");
    StringBuffer output = new StringBuffer();
    output
        .append("Rest uri:\t").append(client.serviceUri)
        .append("\tTable name prefix:\t").append(tableNamePrefix)
        .append("\tResult.bin:\t").append(args[2])
        .append("\tNumber of threads:\t").append(numThreads)
        .append("\tRest application number:\t").append(restAppNum)
        .append("\tRequest number:\t").append(requestNum)
        .append("\tRandom seed length:\t").append(randomLength)
        .append("\tTime:\t").append((allEndTime - allStartTime) / 1000000.0 + " ms")
        .append("\tCache:\t").append(cache);
    printResults(results, output);
  }

  public HorizonClientConcurrent(String serviceUri) {
    this.serviceUri = serviceUri;
  }

  public void cache(String tableName) {
    SelectRequest request = new SelectRequest(tableName, null);
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.postForObject(serviceUri + "/cache", request, SelectResponse.class);
  }

  public static void printResults(List<Future<Results>> results, StringBuffer output)
      throws ExecutionException, InterruptedException {
    ArrayList<Double> timeArray = new ArrayList<>();
    int length = results.size();
    for (int i = 0; i < length; i++) {
      timeArray.add(results.get(i).get().getTime());
    }
    Collections.sort(timeArray, new Comparator<Double>() {
      @Override
      public int compare(Double o1, Double o2) {
        return Double.compare(o1, o2);
      }
    });

    // the time of 90 percent sql are finished
    int time90 = (int) ((timeArray.size()) * 0.9) - 1;
    time90 = time90 < 0 ? 0 : time90;
    // the time of 99 percent sql are finished
    int time99 = (int) ((timeArray.size()) * 0.99) - 1;
    time99 = time99 < 0 ? 0 : time99;
    Double sum = 0.0;
    int avgCount = 0;
    for (int i = 0; i < timeArray.size(); i++) {
      sum = sum + timeArray.get(i);
      avgCount = avgCount + results.get(i).get().getRecords().length;
    }
    output
        .append("\tMin:\t").append(timeArray.get(0) + " ms")
        .append("\tMax: \t").append(timeArray.get(timeArray.size() - 1) + " ms")
        .append("\t90%:\t").append(timeArray.get(time90) + " ms")
        .append("\t99%:\t").append(timeArray.get(time99) + " ms")
        .append("\tAvg: \t").append((sum / timeArray.size()) + " ms")
        .append("\tAvg Count:\t").append(avgCount / timeArray.size());
    System.out.println(output.toString());
  }

  public RestApplication createRestApp(RestTemplate restTemplate, SelectRequest request) {
    return new RestApplication(restTemplate, request, serviceUri);
  }


  static class QueryTask implements Callable<Results> {

    RestApplication restApplication;

    public QueryTask(RestApplication restApplication) {
      this.restApplication = restApplication;
    }

    @Override
    public Results call() throws Exception {
      Long startTime = System.nanoTime();
      String selectId = UUID.randomUUID().toString();
      restApplication.getRequest().setSelectId(selectId);
      Record[] result = restApplication
          .getRestTemplate()
          .postForObject(restApplication.getServiceUri() + "/select",
              restApplication.getRequest(), SelectResponse.class)
          .getRecords();
      Long endTime = System.nanoTime();
      return new Results(result, startTime, endTime);
    }
  }
}

class Results {
  private Record[] records;
  private double startTime;
  private double endTime;
  private double time;

  public Results(Record[] records, long startTime, long endTime) {
    this.records = records;
    this.startTime = startTime;
    this.endTime = endTime;
    this.time = (endTime - startTime) / 1000000.0;
  }

  public Record[] getRecords() {
    return records;
  }

  public double getStartTime() {
    return startTime;
  }

  public double getEndTime() {
    return endTime;
  }

  public double getTime() {
    return time;
  }
}

class RestApplication {
  private RestTemplate restTemplate;
  private SelectRequest request;
  private String serviceUri;

  public RestApplication(RestTemplate restTemplate, String serviceUri) {
    this.restTemplate = restTemplate;
    this.serviceUri = serviceUri;
  }

  public RestApplication(RestTemplate restTemplate, SelectRequest request, String serviceUri) {
    this.restTemplate = restTemplate;
    this.request = request;
    this.serviceUri = serviceUri;
  }

  public RestTemplate getRestTemplate() {
    return restTemplate;
  }

  public SelectRequest getRequest() {
    return request;
  }

  public String getServiceUri() {
    return serviceUri;
  }

  public void setRequest(SelectRequest request) {
    this.request = request;
  }
}