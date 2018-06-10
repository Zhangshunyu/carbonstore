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
package org.apache.carbondata.rest;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.service.client.LocalStoreProxy;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.table.Record;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HorizonController {

  private LocalStoreProxy proxy;

  // We cache following number of tables in the service,
  // table name is "tableN" where N is [0, numTables)
  private int numTables = 100;

  public HorizonController() throws IOException, VisionException {
    String conf = new File(".").getAbsolutePath() + "/select/build/carbonselect/conf";
    proxy = new LocalStoreProxy(conf + "/client/log4j.properties",
        conf + "/model", conf + "/feature", conf + "/carbonselect.properties");
    proxy.cacheTables(numTables);
  }

  @RequestMapping(value = "/select", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> select(@RequestBody SelectRequest request)
      throws VisionException {
    String tableName = request.getTableName();
    if (!tableName.startsWith("table")) {
      return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
    byte[] searchFeature = request.getSearchFeature();
    Record[] result = proxy.select(tableName, searchFeature);
    return new ResponseEntity<>(new SelectResponse(result), HttpStatus.OK);
  }
}
