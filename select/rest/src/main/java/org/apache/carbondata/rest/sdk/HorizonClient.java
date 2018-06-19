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

import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.vision.common.VisionConfiguration;

import org.springframework.web.client.RestTemplate;

public class HorizonClient {

  private String serviceUri;

  // program arguments:
  // uri: "http://localhost:8080"
  // table name: frs_table
  // /home/david/Documents/code/carbonstore/select/build/carbonselect/test/result.bin
  // for test only
  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println("HorizonClient <rest uri> <table name> <result.bin>");
      return;
    }
    HorizonClient client = new HorizonClient(args[0]);
    String tableName = args[1];
    byte[] searchFeature = Utils.generateFeatureSetExample(args[2], 1, 0);

    client.cache(tableName);

    long t1 = System.currentTimeMillis();
    SelectResponse response =
        client.select(tableName, new String[] {"id"}, null, 10, "feature", searchFeature);
    String selectId1 = response.getSelectId();
    Utils.printRecords(response.getRecords());
    System.out.println();

    long t2 = System.currentTimeMillis();
    response =
        client.select(tableName, new String[] {"id"}, null, 10, "feature", searchFeature);
    String selectId2 = response.getSelectId();
    Utils.printRecords(response.getRecords());
    System.out.println();

    long t3 = System.currentTimeMillis();
    response =
        client.select(tableName, new String[] {"id"}, null, 10, "feature", searchFeature);
    String selectId3 = response.getSelectId();
    Utils.printRecords(response.getRecords());
    System.out.println();

    long t4 = System.currentTimeMillis();
    System.out.println("[" + selectId1 + "] end to end taken time: " + (t2 - t1) + " ms");
    System.out.println("[" + selectId2 + "] end to end taken time: " + (t3 - t2) + " ms");
    System.out.println("[" + selectId3 + "] end to end taken time: " + (t4 - t3) + " ms");
  }

  public HorizonClient(String serviceUri) {
    this.serviceUri = serviceUri;
  }

  public void cache(String tableName) {
    SelectRequest request = new SelectRequest(tableName);
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.postForObject(serviceUri + "/cache", request, SelectResponse.class);
  }

  public SelectResponse select(
      String tableName,
      String[] projection,
      String filter,
      int topN,
      String searchColumnName,
      byte[] searchFeature) {
    VisionConfiguration configuration = new VisionConfiguration();
    configuration
        .conf(VisionConfiguration.SELECT_FILTER, filter)
        .conf(VisionConfiguration.SELECT_SEARCH_COLUMN, searchColumnName)
        .conf(VisionConfiguration.SELECT_SEARCH_VECTOR, searchFeature)
        .conf(VisionConfiguration.SELECT_BATCH_SIZE, 100000)
        .conf(VisionConfiguration.SELECT_VECTOR_SIZE, 288);

    SelectRequest request = new SelectRequest(tableName, projection, filter, topN, configuration);
    RestTemplate restTemplate = new RestTemplate();
    return restTemplate.postForObject(serviceUri + "/select", request, SelectResponse.class);
  }
}
