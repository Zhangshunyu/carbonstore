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

package org.apache.carbondata.vision.predict;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.vision.algorithm.AlgorithmExecutor;
import org.apache.carbondata.vision.algorithm.AlgorithmExecutorFactory;
import org.apache.carbondata.vision.common.VisionUtil;
import org.apache.carbondata.vision.feature.FeatureSet;
import org.apache.carbondata.vision.feature.FeatureSetFactory;

public class EntityRecognition {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(EntityRecognition.class.getName());

  private boolean useOffHeap = true;
  private AlgorithmExecutor<float[]> algorithm;
  private int topN;
  private int featureVectorSize;
  private FeatureSet featureSet;
  private String selectId;

  public EntityRecognition(PredictContext context) {
    topN = context.getConf().topN();
    featureVectorSize = context.getConf().vectorSize();
    algorithm = AlgorithmExecutorFactory.getAlgorithmExecutor(context);
    selectId = context.getConf().selectId();
  }

  public void init(int batchSize) {
    if (featureSet == null) {
      featureSet = FeatureSetFactory.createFeatureSet(useOffHeap);
      featureSet.init(((long) batchSize) * featureVectorSize);
      algorithm.init(useOffHeap);
      LOGGER.audit("[" + selectId + "] EntityRecognition batchSize: " + batchSize);
    }
  }

  public void addFeature(byte[] values) {
    featureSet.addFeature(values);
  }

  public List<Object[]> recognition(List<Object[]> rows, int columnCount) {
    long t1 = System.currentTimeMillis();
    float[] result = algorithm.execute(featureSet);
    long t2 = System.currentTimeMillis();
    List<Object[]> topRows = getTopN(result, rows, columnCount);
    long t3 = System.currentTimeMillis();
    LOGGER.audit("[" + selectId + "] recognition taken time: " + (t3 - t1) + " ms, detail: " +
        "t1~(" + (t2 - t1) + ")~t2~(" + (t3 - t2) + ")~t3");
    return topRows;
  }

  private List<Object[]> getTopN(float[] result, List<Object[]> rows, int columnCount) {
    if (result.length > topN) {
      float greatestFloat = VisionUtil.findNthGreatestFloat(result, result.length, topN);
      List<Object[]> topRows = new ArrayList<>(topN);
      for (int i = 0; i < result.length; i++) {
        if (result[i] >= greatestFloat) {
          Object[] row = rows.get(i);
          row[columnCount - 1] = result[i];
          topRows.add(row);
        }
        if (topRows.size() >= topN) {
          break;
        }
      }
      return topRows;
    } else {
      for (int i = 0; i < result.length; i++) {
        rows.get(i)[columnCount] = result[i];
      }
      return rows;
    }
  }

  public void finish() {
    if (featureSet != null) {
      featureSet.finish();
    }
  }

  public String getSelectId() {
    return selectId;
  }
}
