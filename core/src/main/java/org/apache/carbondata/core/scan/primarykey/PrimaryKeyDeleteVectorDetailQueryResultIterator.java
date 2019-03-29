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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.mutate.DeleteDeltaVo;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.TupleIdEnum;
import org.apache.carbondata.core.scan.result.impl.CarbonStreamRecordReader;
import org.apache.carbondata.core.scan.result.iterator.CarbonBatchIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriter;
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl;

/**
 * It reads the data vector batch format
 */
public class PrimaryKeyDeleteVectorDetailQueryResultIterator extends CarbonIterator
    implements CarbonBatchIterator {

  private AbstractQueue<IteratorHolder> recordHolder;

  private PrimaryKeyRowComparator rowComparator;

  private Object[] mergedKey;

  private Object[] newKey;

  private int versionColIndex;

  private int tupleIDIndex;

  private Map<IteratorHolder, DeleteDeltaBlockDetails> deltaBlockDetailsMap;

  private IteratorHolder oldHolder;

  private boolean isMergedKey;

  private List<SegmentUpdateDetails> segmentUpdateDetails = new ArrayList<>();

  private long updateTimeStamp;

  public PrimaryKeyDeleteVectorDetailQueryResultIterator(AbstractQueue<IteratorHolder> recordHolder,
      PrimaryKeyRowComparator rowComparator, int dataLength, int versionColIndex,
      int tupleIDIndex, long updateTimeStamp) {
    this.recordHolder = recordHolder;
    this.rowComparator = rowComparator;
    newKey = new Object[dataLength];
    this.versionColIndex = versionColIndex;
    this.tupleIDIndex = tupleIDIndex;
    deltaBlockDetailsMap = new HashMap<>(recordHolder.size());
    this.updateTimeStamp = updateTimeStamp;

    for (IteratorHolder holder : recordHolder) {
      TableBlockInfo blockInfo =
          holder.getBlockExecutionInfo().getDataBlock().getDataRefNode().getBlockInfo();
      String blockName = CarbonTablePath.getCarbonDataFileName(blockInfo.getFilePath());
      DeleteDeltaBlockDetails deltaBlockDetails = new DeleteDeltaBlockDetails(blockName);
      deltaBlockDetailsMap.put(holder, deltaBlockDetails);
    }
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
        poll.read();
        recordHolder.add(poll);
        continue;
      }
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
          writeDeleteDelta(new String((byte[]) mergedKey[tupleIDIndex]), oldHolder);
          writeDeleteDelta(new String((byte[]) newKey[tupleIDIndex]), poll);
          if ((long) newKey[versionColIndex] > (long) mergedKey[versionColIndex]) {
            for (int i1 = 0; i1 < mergedKey.length; i1++) {
              if (newKey[i1] != null) {
                mergedKey[i1] = newKey[i1];
              }
            }
          }
          isMergedKey = true;
        } else {
          if (isMergedKey) {
            for (int j = 0; j < columnVectors.length; j++) {
              CarbonStreamRecordReader.putRowToColumnBatch(i, mergedKey[j], columnVectors[j]);
            }
            i++;
          }
          isMergedKey = false;
          // TODO find better way to avoid array creation for each object
          System.arraycopy(newKey, 0, mergedKey, 0, newKey.length);

        }
      }
      oldHolder = poll;
    }
    if (i < batchSize && isMergedKey) {
      for (int j = 0; j < columnVectors.length; j++) {
        CarbonStreamRecordReader.putRowToColumnBatch(i, newKey[j], columnVectors[j]);
      }
      i++;
    }
    columnarBatch.setActualSize(i);
  }

  private void writeDeleteDelta(String tid, IteratorHolder holder) {
    DeleteDeltaBlockDetails deltaBlockDetails = deltaBlockDetailsMap.get(holder);
    holder.incrementDeleteRow();
    String offset = CarbonUpdateUtil.getRequiredFieldFromTID(tid, TupleIdEnum.OFFSET);
    String blockletId = CarbonUpdateUtil.getRequiredFieldFromTID(tid, TupleIdEnum.BLOCKLET_ID);
    int pageId =
        Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(tid, TupleIdEnum.PAGE_ID));
    try {
      boolean isValidOffset = deltaBlockDetails.addBlocklet(blockletId, offset, pageId);
      if (!isValidOffset) {
        // TODO
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override public Object next() {
    throw new UnsupportedOperationException("Not supported from here");
  }

  @Override public void close() {
    try {
      String timestamp = String.valueOf(updateTimeStamp);
      for (Map.Entry<IteratorHolder, DeleteDeltaBlockDetails> entry : deltaBlockDetailsMap
          .entrySet()) {
        if (entry.getValue().getBlockletDetails().size() == 0) {
          continue;
        }
        TableBlockInfo blockInfo =
            entry.getKey().getBlockExecutionInfo().getDataBlock().getDataRefNode().getBlockInfo();
        String blockName = CarbonTablePath.getCarbonDataFileName(blockInfo.getFilePath());
        String segmentId = blockInfo.getSegmentId();

        String blockPath =
            blockInfo.getFilePath().substring(0, blockInfo.getFilePath().lastIndexOf("/"));
        String completeBlockName = blockName + CarbonCommonConstants.FACT_FILE_EXT;
        final String blockNameFromTuple =
            blockName.substring(0, blockName.lastIndexOf("-"));
        String deleteDeletaPath =
            CarbonUpdateUtil.getDeleteDeltaFilePath(blockPath, blockNameFromTuple, timestamp);
        CarbonDeleteDeltaWriter carbonDeleteWriter =
            new CarbonDeleteDeltaWriterImpl(deleteDeletaPath,
                FileFactory.getFileType(deleteDeletaPath));

        SegmentUpdateDetails segmentUpdateDetail = new SegmentUpdateDetails();
        segmentUpdateDetail.setBlockName(blockNameFromTuple);
        segmentUpdateDetail.setActualBlockName(completeBlockName);
        segmentUpdateDetail.setSegmentName(segmentId);
        segmentUpdateDetail.setDeleteDeltaEndTimestamp(timestamp);
        segmentUpdateDetail.setDeleteDeltaStartTimestamp(timestamp);
        int prevDeleteRowSize = 0;
        if (entry.getKey().getBlockExecutionInfo().getDeletedRecordsMap() != null) {
          for (DeleteDeltaVo deltaVo : entry.getKey().getBlockExecutionInfo().getDeletedRecordsMap()
              .values()) {
            prevDeleteRowSize += deltaVo.getBitSet().length();
          }
        }
        long totalDeletedRows = prevDeleteRowSize + entry.getKey().getDeleteRowCount();
        segmentUpdateDetail.setDeletedRowsInBlock(String.valueOf(totalDeletedRows));
        if (totalDeletedRows == blockInfo.getDetailInfo().getRowCount()) {
          segmentUpdateDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
        } else {
          // write the delta file
          carbonDeleteWriter.write(entry.getValue());
        }
        this.segmentUpdateDetails.add(segmentUpdateDetail);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<SegmentUpdateDetails> getSegmentUpdateDetails() {
    return segmentUpdateDetails;
  }
}
