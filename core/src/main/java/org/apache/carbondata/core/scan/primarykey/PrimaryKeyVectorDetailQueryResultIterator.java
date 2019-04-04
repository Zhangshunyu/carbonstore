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

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.primarykey.merger.PrimaryKeyMerger;
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

  private Object[] newKey;

  private PrimaryKeyMerger merger;

  public PrimaryKeyVectorDetailQueryResultIterator(AbstractQueue<IteratorHolder> recordHolder,
      int dataLength, PrimaryKeyMerger merger) {
    this.recordHolder = recordHolder;
    newKey = new Object[dataLength];
    this.merger = merger;
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
      if (poll.isDeleted()) {
        if (poll.hasNext()) {
          poll.read();
          recordHolder.add(poll);
        }
        continue;
      }
      for (int i1 = 0; i1 < newKey.length; i1++) {
        newKey[i1] = poll.getCell(i1);
      }
      if (poll.hasNext()) {
        poll.read();
        recordHolder.add(poll);
      }
      if (!merger.isDataAdded()) {
        merger.addFreshRow(newKey);
      } else {
        if (!merger.mergeRow(newKey)) {
          if (!merger.isDeletedRow()) {
            for (int j = 0; j < columnVectors.length; j++) {
              CarbonStreamRecordReader
                  .putRowToColumnBatch(i, merger.getMergedRow()[j], columnVectors[j]);
            }
            i++;
          }
          merger.addFreshRow(newKey);
        }
      }
    }
    if (i < batchSize && !merger.isDeletedRow()) {

      for (int j = 0; j < columnVectors.length; j++) {
        CarbonStreamRecordReader.putRowToColumnBatch(i, merger.getMergedRow()[j], columnVectors[j]);
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
