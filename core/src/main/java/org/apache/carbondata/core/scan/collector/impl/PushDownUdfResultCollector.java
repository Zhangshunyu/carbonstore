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

package org.apache.carbondata.core.scan.collector.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.vision.predict.EntityRecognition;

public class PushDownUdfResultCollector extends DictionaryBasedResultCollector {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(PushDownUdfResultCollector.class.getName());

  private EntityRecognition entityRecognition;

  public PushDownUdfResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    if (blockExecutionInfos.hasPredictContext()) {
      entityRecognition = new EntityRecognition(blockExecutionInfos.getPredictContext());
    }
  }

  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    List<Object[]> listBaseResult = new ArrayList<>(batchSize);
    try {
      String selectId = "";
      if (entityRecognition != null) {
        entityRecognition.init(batchSize);
        selectId = entityRecognition.getSelectId();
      }

      long startTime = System.currentTimeMillis();
      int rowCounter = 0;
      int[] dictKeys;
      byte[][] noDictKeys;
      byte[][] complexKeys;
      int columnCount = queryDimensions.length + queryMeasures.length;

      while (scannedResult.hasNext() && rowCounter < batchSize) {
        Object[] row = new Object[columnCount];
        if (isDimensionExists) {
          dictKeys = scannedResult.getDictionaryKeyIntegerArray();
          noDictKeys = scannedResult.getNoDictionaryKeyArray();
          complexKeys = scannedResult.getComplexTypeKeyArray();
          dictionaryColumnIndex = 0;
          noDictionaryColumnIndex = 0;
          complexTypeColumnIndex = 0;
          for (int i = 0; i < queryDimensions.length; i++) {
            fillDimensionData(scannedResult, dictKeys, noDictKeys, complexKeys,
                comlexDimensionInfoMap, row, i);
          }
        } else {
          scannedResult.incrementCounter();
        }
        if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
          continue;
        }
        fillMeasureData(scannedResult, row);
        listBaseResult.add(row);
        rowCounter++;

        if (entityRecognition != null) {
          entityRecognition.addFeature((byte[]) row[columnCount - 1]);
        }
      }
      long endTime = System.currentTimeMillis();
      LOGGER.audit(
          "[" + selectId + "] collect result taken time: " + (endTime - startTime) + " ms");
      if (entityRecognition != null) {
        return entityRecognition.recognition(listBaseResult, columnCount);
      }
    } finally {
      if (entityRecognition != null) {
        entityRecognition.finish();
      }
    }
    return listBaseResult;
  }
}
