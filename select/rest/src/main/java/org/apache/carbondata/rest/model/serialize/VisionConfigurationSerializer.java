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

package org.apache.carbondata.rest.model.serialize;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.carbondata.vision.common.VisionConfiguration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class VisionConfigurationSerializer extends JsonSerializer<VisionConfiguration> {

  @Override public void serialize(VisionConfiguration configuration, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    Iterator<Map.Entry<String, Object>> iterator = configuration.iterator();
    jsonGenerator.writeStartObject();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> next = iterator.next();
      Object obj = next.getValue();
      if (obj instanceof byte[]) {
        jsonGenerator.writeBinaryField(next.getKey(), (byte[]) next.getValue());
      } else if (obj instanceof String) {
        jsonGenerator.writeStringField(next.getKey(), (String) next.getValue());
      } else if (obj instanceof Integer) {
        jsonGenerator.writeNumberField(next.getKey(), (int) next.getValue());
      } else {
        jsonGenerator.writeObjectField(next.getKey(), next.getValue());
      }
    }
    jsonGenerator.writeEndObject();
  }

}
