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

package org.apache.carbondata.service.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.vision.table.Record;

public class ServiceUtil {

  public static void sortRecords(Record[] records, final int sortColumnIndex) {
    Arrays.sort(records, new Comparator<Record>() {
      @Override public int compare(Record o1, Record o2) {
        float f1 = (float) o1.getValues()[sortColumnIndex];
        float f2 = (float) o2.getValues()[sortColumnIndex];
        return Float.compare(f2, f1);
      }
    });
  }

  public static void parserServerList(String serverList, List<ServerInfo> serverInfos) {
    String[] hostWithPorts = serverList.split(",", -1);
    for (String hostWithPort : hostWithPorts) {
      String[] splits = hostWithPort.split(":", -1);
      if (splits.length == 2) {
        int cores = Runtime.getRuntime().availableProcessors();
        serverInfos.add(
            new ServerInfo(splits[0].trim(), Integer.parseInt(splits[1].trim()), cores));
      }
    }
  }
}
