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
import org.apache.carbondata.vision.common.VisionUtil;
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
    Record[] records = client.select(tableName, searchFeature);
    Utils.printRecords(records);

    long t2 = System.currentTimeMillis();
    records = client.select(tableName, searchFeature);
    Utils.printRecords(records);

    long t3 = System.currentTimeMillis();
    records = client.select(tableName, searchFeature);
    Utils.printRecords(records);

    long t4 = System.currentTimeMillis();
    System.out.println(VisionUtil.printlnTime(t1, t2, t3, t4));
  }

  public HorizonClient(String serviceUri) {
    this.serviceUri = serviceUri;
  }

  public void cache(String tableName) {
    SelectRequest request = new SelectRequest(tableName, null);
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.postForObject(serviceUri + "/cache", request, SelectResponse.class);
  }

  public Record[] select(String tableName, byte[] searchFeature) {
    SelectRequest request = new SelectRequest(tableName, searchFeature);
    RestTemplate restTemplate = new RestTemplate();
    SelectResponse response =
        restTemplate.postForObject(serviceUri + "/select", request, SelectResponse.class);
    return response.getRecords();
  }
}
