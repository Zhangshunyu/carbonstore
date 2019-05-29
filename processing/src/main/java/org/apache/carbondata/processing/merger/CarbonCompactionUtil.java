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
package org.apache.carbondata.processing.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

/**
 * Utility Class for the Compaction Flow.
 */
public class CarbonCompactionUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonCompactionUtil.class.getName());


  /**
   * The processTableBlocks returns a Map<String, List<RawResultIterator>>.
   *
   * This Map has two K,V Pairs. The element with Key=UNSORTED_IDX
   * is the list of all the iterators which yield unsorted data.
   *
   * This Map has two K,V Pairs. The element with Key=SORTED_IDX
   * is the list of all the iterators which yield sorted data.
   */
  public static final String UNSORTED_IDX = "UNSORTED_IDX";
  public static final String SORTED_IDX = "SORTED_IDX";

  /**
   * To create a mapping of Segment Id and TableBlockInfo.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, TaskBlockInfo> createMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList) {

    // stores taskBlockInfo of each segment
    Map<String, TaskBlockInfo> segmentBlockInfoMapping =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);


    for (TableBlockInfo info : tableBlockInfoList) {
      String segId = info.getSegmentId();
      // check if segId is already present in map
      TaskBlockInfo taskBlockInfoMapping = segmentBlockInfoMapping.get(segId);
      // extract task ID from file Path.
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(info.getFilePath());
      // if taskBlockInfo is not there, then create and add
      if (null == taskBlockInfoMapping) {
        taskBlockInfoMapping = new TaskBlockInfo();
        groupCorrespodingInfoBasedOnTask(info, taskBlockInfoMapping, taskNo);
        // put the taskBlockInfo with respective segment id
        segmentBlockInfoMapping.put(segId, taskBlockInfoMapping);
      } else
      {
        groupCorrespodingInfoBasedOnTask(info, taskBlockInfoMapping, taskNo);
      }
    }
    return segmentBlockInfoMapping;

  }

  /**
   * Grouping the taskNumber and list of TableBlockInfo.
   * @param info
   * @param taskBlockMapping
   * @param taskNo
   */
  private static void groupCorrespodingInfoBasedOnTask(TableBlockInfo info,
      TaskBlockInfo taskBlockMapping, String taskNo) {
    // get the corresponding list from task mapping.
    List<TableBlockInfo> blockLists = taskBlockMapping.getTableBlockInfoList(taskNo);
    if (null != blockLists) {
      blockLists.add(info);
    } else {
      blockLists = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      blockLists.add(info);
      taskBlockMapping.addTableBlockInfoList(taskNo, blockLists);
    }
  }

  /**
   * To create a mapping of Segment Id and DataFileFooter.
   *
   * @param tableBlockInfoList
   * @return
   */
  public static Map<String, List<DataFileFooter>> createDataFileFooterMappingForSegments(
      List<TableBlockInfo> tableBlockInfoList, boolean isSortedTable) throws IOException {

    Map<String, List<DataFileFooter>> segmentBlockInfoMapping = new HashMap<>();
    for (TableBlockInfo blockInfo : tableBlockInfoList) {
      List<DataFileFooter> eachSegmentBlocks = new ArrayList<>();
      String segId = blockInfo.getSegmentId();
      DataFileFooter dataFileMatadata = null;
      // check if segId is already present in map
      List<DataFileFooter> metadataList = segmentBlockInfoMapping.get(segId);
      // check to decide whether to read file footer of carbondata file forcefully. This will help
      // in getting the schema last updated time based on which compaction flow is decided that
      // whether it will go to restructure compaction flow or normal compaction flow.
      // This decision will impact the compaction performance so it needs to be decided carefully
      final BlockletInfo blockletInfo = blockInfo.getDetailInfo().getBlockletInfo();
      if (null != blockInfo.getDetailInfo() && (
          blockInfo.getDetailInfo().getSchemaUpdatedTimeStamp() == 0L || null == blockletInfo
              || null == blockletInfo.isSorted() || !blockletInfo.isSorted())) {
        dataFileMatadata = CarbonUtil.readMetadataFile(blockInfo, true);
        if (null == dataFileMatadata.isSorted()) {
          dataFileMatadata.setSorted(isSortedTable);
        }
      } else {
        dataFileMatadata = CarbonUtil.readMetadataFile(blockInfo);
      }
      blockInfo.setDataFileFooter(dataFileMatadata);
      if (null == metadataList) {
        // if it is not present
        eachSegmentBlocks.add(dataFileMatadata);
        segmentBlockInfoMapping.put(segId, eachSegmentBlocks);
      } else {
        // if its already present then update the list.
        metadataList.add(dataFileMatadata);
      }
    }
    return segmentBlockInfoMapping;

  }

  /**
   * Check whether the file to indicate the compaction is present or not.
   * @param metaFolderPath
   * @return
   */
  public static boolean isCompactionRequiredForTable(String metaFolderPath) {
    String minorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.minorCompactionRequiredFile;

    String majorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.majorCompactionRequiredFile;
    try {
      if (FileFactory.isFileExist(minorCompactionStatusFile,
          FileFactory.getFileType(minorCompactionStatusFile)) || FileFactory
          .isFileExist(majorCompactionStatusFile,
              FileFactory.getFileType(majorCompactionStatusFile))) {
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("Exception in isFileExist compaction request file " + e.getMessage(), e);
    }
    return false;
  }

  /**
   * Determine the type of the compaction received.
   * @param metaFolderPath
   * @return
   */
  public static CompactionType determineCompactionType(String metaFolderPath) {
    String minorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.minorCompactionRequiredFile;

    String majorCompactionStatusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.majorCompactionRequiredFile;
    try {
      if (FileFactory.isFileExist(minorCompactionStatusFile,
          FileFactory.getFileType(minorCompactionStatusFile))) {
        return CompactionType.MINOR;
      }
      if (FileFactory.isFileExist(majorCompactionStatusFile,
          FileFactory.getFileType(majorCompactionStatusFile))) {
        return CompactionType.MAJOR;
      }

    } catch (IOException e) {
      LOGGER.error("Exception in determining the compaction request file " + e.getMessage(), e);
    }
    return CompactionType.MINOR;
  }

  /**
   * Delete the compation request file once the compaction is done.
   * @param metaFolderPath
   * @param compactionType
   * @return
   */
  public static boolean deleteCompactionRequiredFile(String metaFolderPath,
      CompactionType compactionType) {
    String compactionRequiredFile;
    if (compactionType.equals(CompactionType.MINOR)) {
      compactionRequiredFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.minorCompactionRequiredFile;
    } else {
      compactionRequiredFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.majorCompactionRequiredFile;
    }
    try {
      if (FileFactory
          .isFileExist(compactionRequiredFile, FileFactory.getFileType(compactionRequiredFile))) {
        if (FileFactory
            .getCarbonFile(compactionRequiredFile, FileFactory.getFileType(compactionRequiredFile))
            .delete()) {
          LOGGER.info("Deleted the compaction request file " + compactionRequiredFile);
          return true;
        } else {
          LOGGER.error("Unable to delete the compaction request file " + compactionRequiredFile);
        }
      } else {
        LOGGER.info("Compaction request file is not present. file is : " + compactionRequiredFile);
      }
    } catch (IOException e) {
      LOGGER.error("Exception in deleting the compaction request file " + e.getMessage(), e);
    }
    return false;
  }

  /**
   * Creation of the compaction request if someother compaction is in progress.
   * @param metaFolderPath
   * @param compactionType
   * @return
   */
  public static boolean createCompactionRequiredFile(String metaFolderPath,
      CompactionType compactionType) {
    String statusFile;
    if (CompactionType.MINOR == compactionType) {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.minorCompactionRequiredFile;
    } else {
      statusFile = metaFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonCommonConstants.majorCompactionRequiredFile;
    }
    try {
      if (!FileFactory.isFileExist(statusFile, FileFactory.getFileType(statusFile))) {
        if (FileFactory.createNewFile(statusFile, FileFactory.getFileType(statusFile))) {
          LOGGER.info("successfully created a compaction required file - " + statusFile);
          return true;
        } else {
          LOGGER.error("Not able to create a compaction required file - " + statusFile);
          return false;
        }
      } else {
        LOGGER.info("Compaction request file : " + statusFile + " already exist.");
      }
    } catch (IOException e) {
      LOGGER.error("Exception in creating the compaction request file " + e.getMessage(), e);
    }
    return false;
  }

  /**
   * This will check if any compaction request has been received for any table.
   *
   * @param carbonTables
   * @return
   */
  public static CarbonTable getNextTableToCompact(CarbonTable[] carbonTables,
      List<CarbonTableIdentifier> skipList) {
    for (CarbonTable ctable : carbonTables) {
      String metadataPath = ctable.getMetadataPath();
      // check for the compaction required file and at the same time exclude the tables which are
      // present in the skip list.
      if (CarbonCompactionUtil.isCompactionRequiredForTable(metadataPath) && !skipList
          .contains(ctable.getCarbonTableIdentifier())) {
        return ctable;
      }
    }
    return null;
  }

  /**
   * This method will add the prepare the max column cardinality map
   *
   * @param columnCardinalityMap
   * @param currentBlockSchema
   * @param currentBlockCardinality
   */
  public static void addColumnCardinalityToMap(Map<String, Integer> columnCardinalityMap,
      List<ColumnSchema> currentBlockSchema, int[] currentBlockCardinality) {
    for (int i = 0; i < currentBlockCardinality.length; i++) {
      // add value to map only if does not exist or new cardinality is > existing value
      String columnUniqueId = currentBlockSchema.get(i).getColumnUniqueId();
      Integer value = columnCardinalityMap.get(columnUniqueId);
      if (null == value) {
        columnCardinalityMap.put(columnUniqueId, currentBlockCardinality[i]);
      } else {
        if (currentBlockCardinality[i] > value) {
          columnCardinalityMap.put(columnUniqueId, currentBlockCardinality[i]);
        }
      }
    }
  }

  /**
   * This method will return the updated cardinality according to the master schema
   *
   * @param columnCardinalityMap
   * @param carbonTable
   * @param updatedColumnSchemaList
   * @return
   */
  public static int[] updateColumnSchemaAndGetCardinality(Map<String, Integer> columnCardinalityMap,
      CarbonTable carbonTable, List<ColumnSchema> updatedColumnSchemaList) {
    List<CarbonDimension> masterDimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    List<Integer> updatedCardinalityList = new ArrayList<>(columnCardinalityMap.size());
    for (CarbonDimension dimension : masterDimensions) {
      Integer value = columnCardinalityMap.get(dimension.getColumnId());
      if (null == value) {
        updatedCardinalityList.add(getDimensionDefaultCardinality(dimension));
      } else {
        updatedCardinalityList.add(value);
      }
      updatedColumnSchemaList.add(dimension.getColumnSchema());

      if (dimension.getNumberOfChild() > 0) {
        fillColumnSchemaListForComplexDims(dimension.getListOfChildDimensions(),
            updatedColumnSchemaList, updatedCardinalityList, columnCardinalityMap);
      }
    }
    // add measures to the column schema list
    List<CarbonMeasure> masterSchemaMeasures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    for (CarbonMeasure measure : masterSchemaMeasures) {
      updatedColumnSchemaList.add(measure.getColumnSchema());
    }
    return ArrayUtils
        .toPrimitive(updatedCardinalityList.toArray(new Integer[updatedCardinalityList.size()]));
  }

  /**
   * This method is to get the chile dimensions of the complex dimension and
   * update the cardinality for all complex dimensions
   *
   * @param carbonDimensionsList
   * @param updatedColumnSchemaList
   * @param updatedCardinalityList
   * @param columnCardinalityMap
   */
  private static void fillColumnSchemaListForComplexDims(List<CarbonDimension> carbonDimensionsList,
      List<ColumnSchema> updatedColumnSchemaList, List<Integer> updatedCardinalityList,
      Map<String, Integer> columnCardinalityMap) {
    for (CarbonDimension carbonDimension : carbonDimensionsList) {
      Integer value = columnCardinalityMap.get(carbonDimension.getColumnId());
      if (null == value) {
        updatedCardinalityList.add(getDimensionDefaultCardinality(carbonDimension));
      } else {
        updatedCardinalityList.add(value);
      }
      updatedColumnSchemaList.add(carbonDimension.getColumnSchema());
      List<CarbonDimension> childDims = carbonDimension.getListOfChildDimensions();
      if (null != childDims && childDims.size() > 0) {
        fillColumnSchemaListForComplexDims(childDims, updatedColumnSchemaList,
            updatedCardinalityList, columnCardinalityMap);
      }
    }
  }

  /**
   * This method will return the default cardinality based on dimension type
   *
   * @param dimension
   * @return
   */
  private static int getDimensionDefaultCardinality(CarbonDimension dimension) {
    int cardinality = 0;
    if (dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      cardinality = Integer.MAX_VALUE;
    } else if (dimension.hasEncoding(Encoding.DICTIONARY)) {
      if (null != dimension.getDefaultValue()) {
        cardinality = CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY + 1;
      } else {
        cardinality = CarbonCommonConstants.DICTIONARY_DEFAULT_CARDINALITY;
      }
    } else {
      cardinality = -1;
    }
    return cardinality;
  }

  /**
   * This method will check for any restructured block in the blocks selected for compaction
   *
   * @param segmentMapping
   * @param dataFileMetadataSegMapping
   * @param tableLastUpdatedTime
   * @return
   */
  public static boolean checkIfAnyRestructuredBlockExists(
      Map<String, TaskBlockInfo> segmentMapping,
      Map<String, List<DataFileFooter>> dataFileMetadataSegMapping,
      long tableLastUpdatedTime) {

    for (Map.Entry<String, TaskBlockInfo> segmentEntry : segmentMapping.entrySet()) {
      String segmentId = segmentEntry.getKey();
      List<DataFileFooter> listMetadata = dataFileMetadataSegMapping.get(segmentId);

      if (isRestructured(listMetadata, tableLastUpdatedTime)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns if any element in the list of DataFileFooter
   * is restructured or not.
   *
   * @param listMetadata
   * @param tableLastUpdatedTime
   * @return
   */
  public static boolean isRestructured(List<DataFileFooter> listMetadata,
      long tableLastUpdatedTime) {
    /*
     * TODO: only in case of add and drop this variable should be true
     */
    for (DataFileFooter dataFileFooter : listMetadata) {
      // if schema modified timestamp is greater than footer stored schema timestamp,
      // it indicates it is a restructured block
      if (tableLastUpdatedTime > dataFileFooter.getSchemaUpdatedTimeStamp()) {
        return true;
      }
    }
    return false;
  }

  // This method will return an Expression(And/Or) for each range based on the datatype
  // This Expression will be passed to each task as a Filter Query to get the data
  public static Expression getFilterExpressionForRange(CarbonColumn[] rangeColumn, Object[] minVal,
      Object[] maxVal, DataType[] dataType) {
    Expression finalExpr = null;
    Expression exp1, exp2;

    for (int i = 0; i < rangeColumn.length; i++) {
      Expression curExpr;
      // In case of null values create an OrFilter expression and
      // for other cases create and AndFilter Expression
      if (null == minVal || minVal[i] == null) {
        // First task
        exp1 = new EqualToExpression(new ColumnExpression(rangeColumn[i].getColName(), dataType[i]),
            new LiteralExpression(null, dataType[i]), true);
        if (null == maxVal || maxVal[i] == null) {
          // If both the min/max values are null, that means, if data contains only
          // null value then pass only one expression as a filter expression
          curExpr = exp1;
        } else {
          exp2 = new LessThanEqualToExpression(new ColumnExpression(rangeColumn[i].getColName(), dataType[i]),
              new LiteralExpression(maxVal[i], dataType[i]));
          if (rangeColumn[i].hasEncoding(Encoding.DICTIONARY)) {
            exp2.setAlreadyResolved(true);
          }
          curExpr = new OrExpression(exp1, exp2);
        }
      } else if (null == maxVal || maxVal[i] == null) {
        // Last task
        curExpr = new GreaterThanExpression(new ColumnExpression(rangeColumn[i].getColName(), dataType[i]),
            new LiteralExpression(minVal[i], dataType[i]));
        if (rangeColumn[i].hasEncoding(Encoding.DICTIONARY)) {
          curExpr.setAlreadyResolved(true);
        }
      } else {
        // Remaining all intermediate ranges
        exp1 = new GreaterThanExpression(new ColumnExpression(rangeColumn[i].getColName(), dataType[i]),
            new LiteralExpression(minVal[i], dataType[i]));
        exp2 = new LessThanEqualToExpression(new ColumnExpression(rangeColumn[i].getColName(), dataType[i]),
            new LiteralExpression(maxVal[i], dataType[i]));
        if (rangeColumn[i].hasEncoding(Encoding.DICTIONARY)) {
          exp2.setAlreadyResolved(true);
          exp1.setAlreadyResolved(true);
        }
        curExpr = new AndExpression(exp1, exp2);
      }
      if (finalExpr == null) {
        finalExpr = curExpr;
      }
//      else {
//        finalExpr = new AndExpression(finalExpr, curExpr);
//      }
    }

    return finalExpr;
  }

  public static Object[][] getOverallMinMax(CarbonInputSplit[] carbonInputSplits,
      CarbonColumn[] rangeCol, boolean isSortCol) {
    Object[] minVal = null;
    Object[] maxVal = null;
    int idx = -1;
    // TODO handle multiple range columns
    DataType[] dataType = new DataType[rangeCol.length];
    boolean[] isDictEncode = new boolean[rangeCol.length];rangeCol[0].hasEncoding(Encoding.DICTIONARY);
    for (int i = 0; i < rangeCol.length; i++) {
      dataType[i] = rangeCol[i].getDataType();
      isDictEncode[i] = rangeCol[i].hasEncoding(Encoding.DICTIONARY);
    }

    Object[][] minMaxVals = new Object[2][rangeCol.length];
    try {
      for (CarbonInputSplit split : carbonInputSplits) {
        DataFileFooter dataFileFooter = null;
        dataFileFooter =
            CarbonUtil.readMetadataFile(CarbonInputSplit.getTableBlockInfo(split), true);
        Object[] minValTemp = new Object[rangeCol.length];
        Object[] maxValTemp = new Object[rangeCol.length];
        for (int k = 0; k < rangeCol.length; k++) {
          List<ColumnSchema> allColumns = dataFileFooter.getColumnInTable();
          for (int i = 0; i < allColumns.size(); i++) {
            if (allColumns.get(i).getColumnName().equalsIgnoreCase(rangeCol[k].getColName())) {
              idx = i;
              break;
            }
          }

          if (isDictEncode[k]) {
            byte[] tempMin = dataFileFooter.getBlockletIndex().getMinMaxIndex().getMinValues()[idx];
            minValTemp[k] = CarbonUtil.getSurrogateInternal(tempMin, 0, tempMin.length);
            byte[] tempMax = dataFileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues()[idx];
            maxValTemp[k] = CarbonUtil.getSurrogateInternal(tempMax, 0, tempMax.length);
          } else {
            minValTemp[k] = dataFileFooter.getBlockletIndex().getMinMaxIndex().getMinValues()[idx];
            maxValTemp[k] = dataFileFooter.getBlockletIndex().getMinMaxIndex().getMaxValues()[idx];
          }
        }
        if (null == minVal) {
          minVal = minValTemp;
          maxVal = maxValTemp;
        } else {
          if (compare(minValTemp, minVal, isDictEncode) <= 0) {
            minVal = minValTemp;
          }
          if (compare(maxValTemp, maxVal, isDictEncode) >= 0) {
            maxVal = maxValTemp;
          }
        }
      }

      for (int i = 0; i < isDictEncode.length; i++) {
        // Based on how min/max value is stored in the footer we change the data
        if (isDictEncode[i]) {
          minMaxVals[0][i] = minVal[i];
          minMaxVals[1][i] = maxVal[i];
        } else {
          if (!isSortCol && (dataType[i] == DataTypes.INT || dataType[i] == DataTypes.LONG)) {
            minMaxVals[0][i] = ByteUtil.toLong((byte[]) minVal[i], 0, ((byte[])minVal[i]).length);
            minMaxVals[1][i] = ByteUtil.toLong((byte[]) maxVal[i], 0, ((byte[])maxVal[i]).length);
          } else if (dataType[i] == DataTypes.DOUBLE) {
            minMaxVals[0][i] = ByteUtil.toDouble((byte[]) minVal[i], 0, ((byte[])minVal[i]).length);
            minMaxVals[1][i] = ByteUtil.toDouble((byte[]) maxVal[i], 0, ((byte[])maxVal[i]).length);
          } else {
            minMaxVals[0][i] = DataTypeUtil
                .getDataBasedOnDataTypeForNoDictionaryColumn((byte[]) minVal[i], dataType[i], true,
                    DataTypeUtil.getDataTypeConverter());
            minMaxVals[1][i] = DataTypeUtil
                .getDataBasedOnDataTypeForNoDictionaryColumn((byte[]) maxVal[i], dataType[i], true,
                    DataTypeUtil.getDataTypeConverter());
          }
        }

      }

    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
    return minMaxVals;
  }

  public static int getTaskCountForSegment(CarbonInputSplit[] splits) {
    Set<String> taskIdSet = new HashSet<>();
    for (CarbonInputSplit split : splits) {
      String taskId = split.taskId;
      taskIdSet.add(taskId);
    }
    return taskIdSet.size();
  }

  private static int compare(Object[] left, Object[] right, boolean[] isDictEncode) {
    int compare = 0;
    for (int i = 0; i < isDictEncode.length; i++) {
      if (isDictEncode[i]) {
        compare = Integer.compare((Integer) left[i], (Integer) right[i]);
        if (compare != 0) {
          return compare;
        }
      } else {
        compare = ByteUtil.compare((byte[]) left[i], (byte[]) right[i]);
        if (compare != 0) {
          return compare;
        }
      }
    }
    return compare;
  }

  /**
   * Returns if the DataFileFooter containing carbondata file contains
   * sorted data or not.
   *
   * @param table
   * @param footer
   * @return
   */
  public static boolean isSortedByCurrentSortColumns(CarbonTable table, DataFileFooter footer) {
    if (footer.isSorted()) {
      // When sort_columns is modified, it will be consider as no_sort also.
      List<CarbonDimension> sortColumnsOfSegment = new ArrayList<>();
      for (ColumnSchema column : footer.getColumnInTable()) {
        if (column.isDimensionColumn() && column.isSortColumn()) {
          sortColumnsOfSegment.add(new CarbonDimension(column, -1, -1, -1));
        }
      }
      if (sortColumnsOfSegment.size() < table.getNumberOfSortColumns()) {
        return false;
      }
      List<CarbonDimension> sortColumnsOfTable = new ArrayList<>();
      for (CarbonDimension dimension : table.getDimensions()) {
        if (dimension.isSortColumn()) {
          sortColumnsOfTable.add(dimension);
        }
      }
      int sortColumnNums = sortColumnsOfTable.size();
      if (sortColumnsOfSegment.size() < sortColumnNums) {
        return false;
      }
      // compare sort_columns
      for (int i = 0; i < sortColumnNums; i++) {
        if (!RestructureUtil
            .isColumnMatches(table.isTransactionalTable(), sortColumnsOfTable.get(i),
                sortColumnsOfSegment.get(i))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

}
