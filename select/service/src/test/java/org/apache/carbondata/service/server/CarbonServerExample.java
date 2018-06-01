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

package org.apache.carbondata.service.server;

import java.io.IOException;

import org.apache.carbondata.service.client.ExampleUtils;
import org.apache.carbondata.vision.common.VisionConfiguration;

public class CarbonServerExample {

  // vm argument: -Djava.library.path=/home/david/Documents/code/carbonstore/ai/vision-native/build/lib
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: CarbonServerExample <log4j file> <properties file>");
      return;
    }
    ExampleUtils.initLog4j(args[0]);
    VisionConfiguration conf = new VisionConfiguration();
    conf.load(args[1]);
    new CarbonServer(conf).start();
  }

}
