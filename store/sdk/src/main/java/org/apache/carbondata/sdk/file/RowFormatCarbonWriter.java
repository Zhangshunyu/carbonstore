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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileWriter;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletIndex;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.streaming.CarbonStreamOutputFormat;
import org.apache.carbondata.streaming.CarbonStreamRecordWriter;
import org.apache.carbondata.streaming.index.StreamFileIndex;
import org.apache.carbondata.streaming.segment.StreamSegment;

import static org.apache.carbondata.streaming.segment.StreamSegment.updateStreamFileIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;


public class RowFormatCarbonWriter extends CarbonWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(RowFormatCarbonWriter.class.getName());

  private CarbonStreamRecordWriter recordWriter;

  private int blockletRowCount;

  private CarbonLoadModel loadModel;

  private String segmentId;

  private Configuration hadoopConf;

  private IndexHeader indexHeader;

  public RowFormatCarbonWriter(CarbonLoadModel loadModel, Configuration hadoopConf)
      throws IOException {
    this.loadModel = loadModel;
    this.hadoopConf = hadoopConf;
    createWriter(loadModel, hadoopConf);
  }

  private void createWriter(CarbonLoadModel loadModel, Configuration hadoopConf)
      throws IOException {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    segmentId = StreamSegment.open(carbonTable);
    String segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath(), segmentId);
    if (FileFactory.isFileExist(segmentDir) && 1024 * 1024 <= FileFactory
        .getDirectorySize(segmentDir)) {
      segmentId = StreamSegment.close(carbonTable, segmentId);
      String segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath(), segmentId);
      FileFactory.mkdirs(segmentPath, FileFactory.getFileType(segmentDir));
      loadModel.setTaskNo(null);
      if (recordWriter != null) {
        recordWriter.close(null);
      }
    }
    CarbonStreamOutputFormat.setSegmentId(hadoopConf, segmentId);
    Random random = new Random();
    if (loadModel.getTaskNo() == null) {
      loadModel.setTaskNo(random.nextInt(Integer.MAX_VALUE) + "");
      loadModel.setFactTimeStamp(System.currentTimeMillis());
      recordWriter =
          new CarbonStreamRecordWriter(new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()),
              loadModel);
    }
  }

  @Override public void write(Object record) throws IOException {
    recordWriter.write(null, record);
    blockletRowCount++;
  }

  @Override public void flushBatch() throws IOException {
    if (recordWriter.appendBlockletToDataFile()) {
      StreamFileIndex streamBlockIndex = createStreamBlockIndex(recordWriter.getFileName(),
          recordWriter.getBatchMinMaxIndexWithoutMerge(), blockletRowCount);
      CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
      List<CarbonMeasure> measures = carbonTable.getMeasures();
      DataType[] msrDataTypes = new DataType[measures.size()];
      for (int i = 0; i < measures.size(); i++) {
        msrDataTypes[i] = measures.get(i).getDataType();
      }
      // update data file info in index file
      updateIndexFile(carbonTable.getTablePath(), streamBlockIndex, msrDataTypes);
      blockletRowCount = 0;
    }
    createWriter(loadModel, hadoopConf);
  }

  @Override public void close() throws IOException {
    flushBatch();
    recordWriter.close(null);
  }

  private void updateIndexFile(String tablePath, StreamFileIndex fileIndex, DataType[] msrDataTypes)
      throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(tablePath);
    String filePath = CarbonTablePath
        .getCarbonIndexFilePath(tablePath, loadModel.getTaskNo(), segmentId, "0", "0",
            ColumnarFormatVersion.R1);
    // update min/max index
    Map<String, StreamFileIndex> indexMap = new HashMap<>();
    indexMap.put(fileIndex.getFileName(), fileIndex);
    updateStreamFileIndex(indexMap, filePath, fileType, msrDataTypes);
    if (indexHeader == null) {
      CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
      int[] cardinality =
          new int[carbonTable.getTableInfo().getFactTable().getListOfColumns().size()];
      List<ColumnSchema> columnSchemaList = AbstractFactDataWriter
          .getColumnSchemaListAndCardinality(new ArrayList<Integer>(), cardinality,
              carbonTable.getTableInfo().getFactTable().getListOfColumns());
      indexHeader = CarbonMetadataUtil.getIndexHeader(cardinality, columnSchemaList, 0, 0);
      indexHeader.setIs_sort(false);
    }
    String tempFilePath = filePath + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    CarbonIndexFileWriter writer = new CarbonIndexFileWriter();
    String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
    try {
      writer.openThriftWriter(tempFilePath);
      if (false) {
        writer.writeThrift(indexHeader);
      }
      BlockIndex blockIndex;
      for (Map.Entry<String, StreamFileIndex> entry : indexMap.entrySet()) {
        blockIndex = new BlockIndex();
        blockIndex.setFile_name(entry.getKey());
        blockIndex.setFile_size(
            FileFactory.getCarbonFile(new Path(segmentPath, entry.getKey()).toString()).getSize());
        blockIndex.setOffset(-1);
        // set min/max index
        BlockletIndex blockletIndex = new BlockletIndex();
        blockIndex.setBlock_index(blockletIndex);
        StreamFileIndex streamFileIndex = indexMap.get(blockIndex.getFile_name());
        if (streamFileIndex != null) {
          blockletIndex.setMin_max_index(
              CarbonMetadataUtil.convertMinMaxIndex(streamFileIndex.getMinMaxIndex()));
          blockIndex.setNum_rows(streamFileIndex.getRowCount());
        } else {
          blockIndex.setNum_rows(-1);
        }
        // write block index
        writer.writeThrift(blockIndex);
      }
      writer.close();
      CarbonFile tempFile = FileFactory.getCarbonFile(tempFilePath, fileType);
      if (!tempFile.renameForce(filePath)) {
        throw new IOException(
            "temporary file renaming failed, src=" + tempFilePath + ", dest=" + filePath);
      }
    } catch (IOException ex) {
      try {
        writer.close();
      } catch (IOException t) {
        LOGGER.error(t);
      }
      throw ex;
    }
  }

  /**
   * create a StreamBlockIndex from the SimpleStatsResult array
   */
  private static StreamFileIndex createStreamBlockIndex(String fileName,
      BlockletMinMaxIndex minMaxIndex, int blockletRowCount) {
    StreamFileIndex streamFileIndex = new StreamFileIndex(fileName, minMaxIndex, blockletRowCount);
    return streamFileIndex;
  }
}
