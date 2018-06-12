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

package org.apache.carbondata.service.service;

import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface PredictService extends VersionedProtocol {

  public long versionID = 1L;

  boolean loadLibrary(String libName);

  Model loadModel(String modelPath) throws VisionException;

  byte[] getTable(Table table) throws VisionException;

  void cacheTable(Table table, int cacheLevel) throws VisionException;

  Record[] search(CarbonMultiBlockSplit split, PredictContext context) throws VisionException;
}
