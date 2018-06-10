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

import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.service.Utils;
import org.apache.carbondata.vision.table.Record;

import org.springframework.web.client.RestTemplate;

public class HorizonClient {
  private String serviceUri;

  // for test only
  public static void main(String[] args) {
    HorizonClient client = new HorizonClient("http://localhost:8080/select");
    Record[] records = client.select("table0", new byte[]{1,23});
    Utils.printRecords(records);
  }

  public HorizonClient(String serviceUri) {
    this.serviceUri = serviceUri;
  }

  public Record[] select(String tableName, byte[] searchFeature) {
    SelectRequest request = new SelectRequest(tableName, searchFeature);
    RestTemplate restTemplate = new RestTemplate();
    SelectResponse response = restTemplate.postForObject(serviceUri, request, SelectResponse.class);
    return response.getRecords();
  }
}
