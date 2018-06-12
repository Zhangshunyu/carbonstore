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

package org.apache.carbondata.vision.algorithm.impl;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.vision.algorithm.AlgorithmExecutor;
import org.apache.carbondata.vision.common.OffHeapAllocator;
import org.apache.carbondata.vision.feature.Feature;
import org.apache.carbondata.vision.feature.FeatureSet;
import org.apache.carbondata.vision.feature.impl.FeatureBytes;
import org.apache.carbondata.vision.predict.PredictContext;

public class KNNSearch implements AlgorithmExecutor<float[]> {

  private static LogService LOGGER = LogServiceFactory.getLogService(KNNSearch.class.getName());

  private String selectId;
  private Feature searchFeature;
  private String modelPath;
  private boolean useOffHeap;

  public KNNSearch(PredictContext context) {
    selectId = context.getConf().selectId();
    searchFeature = new FeatureBytes();
    searchFeature.setValue(context.getConf().searchVector());
    modelPath = context.getModel().getModelFilePath();
  }

  @Override public void init(boolean useOffHeap) {
    this.useOffHeap = useOffHeap;
  }

  @Override public float[] execute(FeatureSet featureSet) {
    float[] distances = new float[featureSet.getLength()];
    if (useOffHeap) {
      long searchAddress = searchFeature.getAddress();
      long featureSetAddress = featureSet.getAddress();
      LOGGER.audit("[" + selectId + "] KNNSearch call calcDistanceSetNative");
      calcDistanceSetNative(searchAddress, featureSetAddress, distances, modelPath);
      LOGGER.audit("[" + selectId + "] KNNSearch finished.");
      OffHeapAllocator.freeMemory(searchAddress);
    } else {
      LOGGER.audit("[" + selectId + "] KNNSearch call calcDistanceSet");
      calcDistanceSet(searchFeature.getBytes(), featureSet.getBytes(), distances, modelPath);
      LOGGER.audit("[" + selectId + "] KNNSearch finished.");
    }
    return distances;
  }

  @Override public String getShortName() {
    return "KNNSearch";
  }

  @Override public void finish() {

  }

  public native int calcDistanceSet(byte[] search, byte[] featureSet, float[] output,
      String modelPath);

  public native int calcDistanceSetNative(long search, long featureSet, float[] output,
      String modelPath);

}
