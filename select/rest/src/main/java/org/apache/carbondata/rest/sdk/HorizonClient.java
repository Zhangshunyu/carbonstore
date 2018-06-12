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
import java.util.UUID;

import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.vision.table.Record;

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
    String selectId1 = UUID.randomUUID().toString();
    Record[] records = client.select(tableName, searchFeature, selectId1);
    Utils.printRecords(records);

    long t2 = System.currentTimeMillis();
    String selectId2 = UUID.randomUUID().toString();
    records = client.select(tableName, searchFeature, selectId2);
    Utils.printRecords(records);

    long t3 = System.currentTimeMillis();
    String selectId3 = UUID.randomUUID().toString();
    records = client.select(tableName, searchFeature, selectId3);
    Utils.printRecords(records);

    long t4 = System.currentTimeMillis();
    System.out.println("[" + selectId1 + "] end to end taken time: " + (t2 - t1) + " ms");
    System.out.println("[" + selectId2 + "] end to end taken time: " + (t3 - t2) + " ms");
    System.out.println("[" + selectId3 + "] end to end taken time: " + (t4 - t3) + " ms");
  }

  public HorizonClient(String serviceUri) {
    this.serviceUri = serviceUri;
  }

  public void cache(String tableName) {
    SelectRequest request = new SelectRequest(tableName, null);
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.postForObject(serviceUri + "/cache", request, SelectResponse.class);
  }

  public Record[] select(String tableName, byte[] searchFeature, String selectId) {
    SelectRequest request = new SelectRequest(tableName, searchFeature);
    request.setSelectId(selectId);
    RestTemplate restTemplate = new RestTemplate();
    SelectResponse response =
        restTemplate.postForObject(serviceUri + "/select", request, SelectResponse.class);
    return response.getRecords();
  }
}
