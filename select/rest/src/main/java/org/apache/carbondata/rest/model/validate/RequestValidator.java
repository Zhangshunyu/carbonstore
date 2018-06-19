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

package org.apache.carbondata.rest.model.validate;

import org.apache.carbondata.rest.model.SelectRequest;
import org.apache.carbondata.vision.common.VisionException;

public class RequestValidator {

  public static void validateForSelect(SelectRequest request) throws VisionException {
    if (request == null) {
      throw new VisionException("SelectRequest should not be null");
    }
    if (request.getTableName() == null || request.getTableName().isEmpty()) {
      throw new VisionException("tableName is invalid");
    }

    if (request.getConfiguration() == null) {
      throw new VisionException("configuration is invalid");
    }

    if (request.getConfiguration().searchVector() == null ||
        request.getConfiguration().searchVector().length != 288) {
      throw new VisionException("searchFeature is invalid");
    }
    if (request.getProjection() == null || request.getProjection().length == 0) {
      throw new VisionException("projection is invalid");
    }
  }

  public static void validateForCache(SelectRequest request) throws VisionException {
    if (request == null) {
      throw new VisionException("SelectRequest should not be null");
    }
    if (request.getTableName() == null || request.getTableName().isEmpty()) {
      throw new VisionException("tableName is invalid");
    }
  }

}
