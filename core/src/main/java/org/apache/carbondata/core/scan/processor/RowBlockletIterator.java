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
package org.apache.carbondata.core.scan.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.impl.CarbonStreamRecordReader;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public class RowBlockletIterator extends DataBlockIterator {

  private CarbonStreamRecordReader recordReader;

  public RowBlockletIterator(BlockExecutionInfo blockExecutionInfo, FileReader fileReader,
      int batchSize, QueryStatisticsModel queryStatisticsModel, ExecutorService executorService) {
    super(blockExecutionInfo, fileReader, batchSize, queryStatisticsModel, executorService);
    recordReader = new CarbonStreamRecordReader(blockExecutionInfo,
        blockExecutionInfo.isVectorBatchCollector(), false);
  }

  @Override public List<Object[]> next() {
    List<Object[]> collectedResult = new ArrayList<>(batchSize);
    while (recordReader.hasNext() && collectedResult.size() < batchSize) {
      collectedResult.add(recordReader.next());
    }
    return collectedResult;
  }

  @Override public boolean hasNext() {
    return recordReader.hasNext();
  }

  public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    try {
      recordReader.next(columnarBatch);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Close the resources
   */
  public void close() {
    // free the current scanned result
    recordReader.close();
  }
}