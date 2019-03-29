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
package org.apache.carbondata.core.scan.primarykey;

import java.util.AbstractQueue;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.impl.CarbonStreamRecordReader;
import org.apache.carbondata.core.scan.result.iterator.CarbonBatchIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

/**
 * It reads the data vector batch format
 */
public class PrimaryKeyVectorDetailQueryResultIterator extends CarbonIterator
    implements CarbonBatchIterator {

  private AbstractQueue<IteratorHolder> recordHolder;

  private PrimaryKeyRowComparator rowComparator;

  private Object[] mergedKey;

  private Object[] newKey;

  private int versionColIndex;

  public PrimaryKeyVectorDetailQueryResultIterator(AbstractQueue<IteratorHolder> recordHolder,
      PrimaryKeyRowComparator rowComparator, int dataLength, int versionColIndex) {
    this.recordHolder = recordHolder;
    this.rowComparator = rowComparator;
    newKey = new Object[dataLength];
    this.versionColIndex = versionColIndex;
  }

  @Override public boolean hasNext() {
    return recordHolder.size() > 0;
  }

  @Override public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    int batchSize = columnarBatch.getBatchSize();
    CarbonColumnVector[] columnVectors = columnarBatch.columnVectors;
    int i = 0;
    while (i < batchSize && hasNext()) {
      IteratorHolder poll = this.recordHolder.poll();
      for (int i1 = 0; i1 < newKey.length; i1++) {
        newKey[i1] = poll.getCell(i1);
      }
      if (poll.hasNext()) {
        poll.read();
        recordHolder.add(poll);
      }
      if (mergedKey == null) {
        mergedKey = new Object[newKey.length];
        System.arraycopy(newKey, 0, mergedKey, 0, newKey.length);
      } else {
        if (rowComparator.compare(mergedKey, newKey) == 0) {
          if ((long) newKey[versionColIndex] > (long) mergedKey[versionColIndex]) {
            for (int i1 = 0; i1 < mergedKey.length; i1++) {
              if (newKey[i1] != null) {
                mergedKey[i1] = newKey[i1];
              }
            }
          }
        } else {
          for (int j = 0; j < columnVectors.length; j++) {
            CarbonStreamRecordReader.putRowToColumnBatch(i, mergedKey[j], columnVectors[j]);
          }
          // TODO find better way to avoid array creation for each object
          System.arraycopy(newKey, 0, mergedKey, 0, newKey.length);
          i++;
        }
      }
    }
    if (i < batchSize) {
      for (int j = 0; j < columnVectors.length; j++) {
        CarbonStreamRecordReader.putRowToColumnBatch(i, newKey[j], columnVectors[j]);
      }
      i++;
    }
    columnarBatch.setActualSize(i);
  }

  @Override public Object next() {
    throw new UnsupportedOperationException("Not supported from here");
  }

  @Override public void close() {

  }
}
