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

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.rest.model.SelectResponse;
import org.apache.carbondata.rest.model.validate.RequestValidator;
import org.apache.carbondata.service.client.LocalStoreProxy;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.table.Record;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController public class HorizonController {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(HorizonController.class.getName());

  private LocalStoreProxy proxy;

  public HorizonController() throws IOException, VisionException {
    String homePath = System.getProperty("carbonselect.rest.home");
    if (homePath == null || homePath.isEmpty()) {
      throw new VisionException("can not find carbonselect.rest.home");
    }
    proxy = new LocalStoreProxy(homePath + "/conf/client/log4j.properties",
        homePath + "/lib/third/intellifData", homePath + "/conf/client/carbonselect.properties");
  }

  @RequestMapping(value = "/cache", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> cache(@RequestBody SelectRequest request)
      throws VisionException {
    RequestValidator.validateForCache(request);
    proxy.cacheTable(request.getTableName());
    LOGGER.audit("cached table: " + request.getTableName());
    return new ResponseEntity<>(new SelectResponse(new Record[0]), HttpStatus.OK);
  }

  @RequestMapping(value = "/select", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<SelectResponse> select(@RequestBody SelectRequest request)
      throws VisionException {
    long start = System.currentTimeMillis();
    RequestValidator.validateForSelect(request);
    Record[] result = proxy.select(request.getTableName(), request.getSearchFeature());
    long end = System.currentTimeMillis();
    LOGGER.audit(
        "select table: " + request.getTableName() + ", taken time: " + (end - start) + " ms");
    return new ResponseEntity<>(new SelectResponse(result), HttpStatus.OK);
  }

}
