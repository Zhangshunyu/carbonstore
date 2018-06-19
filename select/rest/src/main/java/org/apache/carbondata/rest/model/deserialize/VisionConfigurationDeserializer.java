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

package org.apache.carbondata.rest.model.deserialize;

import java.io.IOException;

import org.apache.carbondata.vision.common.VisionConfiguration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class VisionConfigurationDeserializer extends JsonDeserializer<VisionConfiguration> {

  @Override public VisionConfiguration deserialize(JsonParser jsonParser,
      DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

    VisionConfiguration configuration = new VisionConfiguration();
    while (!jsonParser.isClosed()) {
      JsonToken jsonToken = jsonParser.nextToken();

      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        String fieldName = jsonParser.getCurrentName();

        jsonToken = jsonParser.nextToken();

        if (VisionConfiguration.SELECT_SEARCH_VECTOR.equalsIgnoreCase(fieldName)) {
          configuration.conf(VisionConfiguration.SELECT_SEARCH_VECTOR, jsonParser.getBinaryValue());
        } else if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
          configuration.conf(fieldName, jsonParser.getIntValue());
        } else {
          configuration.conf(fieldName, jsonParser.getValueAsString());
        }
      }
    }
    return configuration;
  }
}
