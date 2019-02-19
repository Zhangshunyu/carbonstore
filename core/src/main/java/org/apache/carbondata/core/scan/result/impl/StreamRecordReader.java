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

package org.apache.carbondata.core.scan.result.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.DataTypeConverter;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.FileHeader;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.Path;

/**
 * Stream row record reader
 */
public class StreamRecordReader extends CarbonIterator<Object[]> {

  // metadata
  private CarbonColumn[] storageColumns;
  private boolean[] isRequired;
  private DataType[] measureDataTypes;
  private int dimensionCount;
  private int measureCount;

  // input
  protected StreamBlockletReader input;
  protected boolean isFirstRow = true;

  // decode data
  private BitSet allNonNull;
  private boolean[] isNoDictColumn;
  private DirectDictionaryGenerator[] directDictionaryGenerators;
  private String compressorName;

  // vectorized reader
  protected boolean isFinished = false;

  // filter
  protected FilterExecuter filter;
  private boolean[] isFilterRequired;
  private Object[] filterValues;
  protected RowIntf filterRow;
  private int[] filterMap;

  // output
  private boolean[] isProjectionRequired;
  protected int[] projectionMap;
  protected Object[] outputValues;

  // empty project, null filter
  protected boolean skipScanData;

  // return raw row for handoff
  private boolean useRawRow = false;

  protected BlockExecutionInfo blockExecutionInfo;

  protected CarbonColumn[] projection;

  protected boolean notConsumed;

  protected DataTypeConverter dataTypeConverter;

  private GenericQueryType[] queryTypes;

  public StreamRecordReader(BlockExecutionInfo blockExecutionInfo, boolean useRawRow) {
    this.blockExecutionInfo = blockExecutionInfo;
    this.useRawRow = useRawRow;
    initialize();
  }

  public void initialize() {
    SegmentProperties segmentProperties = blockExecutionInfo.getDataBlock().getSegmentProperties();
    dataTypeConverter = DataTypeUtil.getDataTypeConverter();

    List<CarbonDimension> dimensions = segmentProperties.getDimensions();
    List<CarbonDimension> complexDimensions = segmentProperties.getComplexDimensions();
    dimensions.addAll(complexDimensions);
    dimensionCount = dimensions.size();
    List<CarbonMeasure> measures = segmentProperties.getMeasures();
    measureCount = measures.size();
    List<CarbonColumn> carbonColumnList = getStreamStorageOrderColumn(dimensions, measures);
    storageColumns = carbonColumnList.toArray(new CarbonColumn[carbonColumnList.size()]);
    isNoDictColumn = getNoDictionaryMapping(storageColumns);
    directDictionaryGenerators = new DirectDictionaryGenerator[storageColumns.length];
    for (int i = 0; i < storageColumns.length; i++) {
      if (storageColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(storageColumns[i].getDataType());
      }
    }
    measureDataTypes = new DataType[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] = storageColumns[dimensionCount + i].getDataType();
    }

    // decode data
    allNonNull = new BitSet(storageColumns.length);

    isRequired = new boolean[storageColumns.length];

    // set the filter to the query model in order to filter blocklet before scan
    boolean[] isFiltlerDimensions = blockExecutionInfo.getIsFilterDimensions();
    boolean[] isFiltlerMeasures = blockExecutionInfo.getIsFilterMeasures();

    isFilterRequired = new boolean[storageColumns.length];
    filterMap = new int[storageColumns.length];
    for (int i = 0; i < storageColumns.length; i++) {
      if (storageColumns[i].isDimension()) {
        if (isFiltlerDimensions[storageColumns[i].getOrdinal()]) {
          isRequired[i] = true;
          isFilterRequired[i] = true;
          filterMap[i] = storageColumns[i].getOrdinal();
        }
      } else {
        if (isFiltlerMeasures[storageColumns[i].getOrdinal()]) {
          isRequired[i] = true;
          isFilterRequired[i] = true;
          filterMap[i] =
              segmentProperties.getLastDimensionColOrdinal() + storageColumns[i].getOrdinal();
        }
      }
    }

    isProjectionRequired = new boolean[storageColumns.length];
    projectionMap = new int[storageColumns.length];

    projection =
        new CarbonColumn[blockExecutionInfo.getProjectionDimensions().length + blockExecutionInfo
            .getProjectionMeasures().length];
    int k = 0;
    for (ProjectionDimension dimension : blockExecutionInfo.getProjectionDimensions()) {
      projection[dimension.getOrdinal()] = dimension.getDimension();
    }
    for (ProjectionMeasure measure : blockExecutionInfo.getProjectionMeasures()) {
      projection[measure.getOrdinal()] = measure.getMeasure();
    }
    queryTypes = new GenericQueryType[projection.length];
    Map<Integer, GenericQueryType> comlexDimensionInfoMap =
        blockExecutionInfo.getComlexDimensionInfoMap();
    for (int j = 0; j < projection.length; j++) {
      for (int i = 0; i < storageColumns.length; i++) {
        if (storageColumns[i].getColName().equals(projection[j].getColName())) {
          isRequired[i] = true;
          isProjectionRequired[i] = true;
          projectionMap[i] = j;
          queryTypes[storageColumns[i].getOrdinal()] =
              comlexDimensionInfoMap.get(storageColumns[i].getOrdinal());
          break;
        }
      }
    }
    filter = blockExecutionInfo.getFilterExecuterTree();
    // initialize filter
    if (projection.length == 0) {
      skipScanData = true;
    }
  }

  /**
   * This method will give storage order column list
   */
  public List<CarbonColumn> getStreamStorageOrderColumn(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures) {
    List<CarbonColumn> columnList = new ArrayList<>(dimensions.size() + measures.size());
    List<CarbonColumn> complexDimensionList = new ArrayList<>(dimensions.size());
    for (CarbonColumn column : dimensions) {
      if (column.isComplex()) {
        complexDimensionList.add(column);
      } else {
        columnList.add(column);
      }
    }
    columnList.addAll(complexDimensionList);
    for (CarbonColumn column : measures) {
      if (!(column.getColName().equals("default_dummy_measure"))) {
        columnList.add(column);
      }
    }
    return columnList;
  }

  public static boolean[] getNoDictionaryMapping(CarbonColumn[] carbonColumns) {
    List<Boolean> noDictionaryMapping = new ArrayList<Boolean>();
    for (CarbonColumn column : carbonColumns) {
      // for  complex type need to break the loop
      if (column.isComplex()) {
        break;
      }
      if (!column.hasEncoding(Encoding.DICTIONARY) && column.isDimension()) {
        noDictionaryMapping.add(true);
      } else if (column.isDimension()) {
        noDictionaryMapping.add(false);
      }
    }
    return ArrayUtils
        .toPrimitive(noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }

  private byte[] getSyncMarker(String filePath) throws IOException {
    CarbonHeaderReader headerReader = new CarbonHeaderReader(filePath);
    FileHeader header = headerReader.readHeader();
    // legacy store does not have this member
    if (header.isSetCompressor_name()) {
      compressorName = header.getCompressor_name();
    } else {
      compressorName = CompressorFactory.NativeSupportedCompressor.SNAPPY.getName();
    }
    return header.getSync_marker();
  }

  protected void initializeAtFirstRow() throws IOException {
    filterValues = new Object[
        blockExecutionInfo.getDataBlock().getSegmentProperties().getLastDimensionColOrdinal()
            + measureCount];
    filterRow = new RowImpl();
    filterRow.setValues(filterValues);

    outputValues = new Object[projection.length];
    TableBlockInfo blockInfo = blockExecutionInfo.getDataBlock().getDataRefNode().getBlockInfo();
    Path file = new Path(blockInfo.getFilePath());

    byte[] syncMarker = getSyncMarker(file.toString());
    DataInputStream fileIn = FileFactory.getDataInputStream(blockInfo.getFilePath(),
        FileFactory.getFileType(blockInfo.getFilePath()));
    input = new StreamBlockletReader(syncMarker, fileIn, blockInfo.getBlockLength(),
        blockInfo.getBlockOffset() == 0, compressorName);
  }

  /**
   * check next Row
   */
  protected boolean nextRow() throws IOException {
    // read row one by one
    try {
      outputValues = new Object[projection.length];
      boolean hasNext;
      boolean scanMore = false;
      do {
        hasNext = input.hasNext();
        if (hasNext) {
          if (skipScanData) {
            input.nextRow();
            scanMore = false;
          } else {
            if (useRawRow) {
              // read raw row for streaming handoff which does not require decode raw row
              readRawRowFromStream();
            } else {
              readRowFromStream();
            }
            if (null != filter) {
              scanMore = !filter.applyFilter(filterRow,
                  blockExecutionInfo.getDataBlock().getSegmentProperties()
                      .getLastDimensionColOrdinal());
            } else {
              scanMore = false;
            }
          }
        } else {
          if (input.nextBlocklet()) {
            BlockletHeader header = input.readBlockletHeader();
            if (isScanRequired(header)) {
              if (skipScanData) {
                input.skipBlockletData(false);
              } else {
                input.readBlockletData(header);
              }
            } else {
              input.skipBlockletData(true);
            }
            scanMore = true;
          } else {
            isFinished = true;
            scanMore = false;
          }
        }
      } while (scanMore);
      return hasNext;
    } catch (FilterUnsupportedException e) {
      throw new IOException("Failed to filter row in detail reader", e);
    }
  }

  @Override public boolean hasNext() {
    try {
      if (isFirstRow) {
        isFirstRow = false;
        initializeAtFirstRow();

      }
      if (isFinished) {
        return false;
      }
      if (!notConsumed) {
        notConsumed = true;
        return nextRow();
      } else {
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override public Object[] next() {
    notConsumed = false;
    return outputValues;
  }

  protected boolean isScanRequired(BlockletHeader header) {
    if (filter != null && header.getBlocklet_index() != null) {
      BlockletMinMaxIndex minMaxIndex = CarbonMetadataUtil
          .convertExternalMinMaxIndex(header.getBlocklet_index().getMin_max_index());
      if (minMaxIndex != null) {
        BitSet bitSet = filter
            .isScanRequired(minMaxIndex.getMaxValues(), minMaxIndex.getMinValues(),
                minMaxIndex.getIsMinMaxSet());
        if (bitSet.isEmpty()) {
          return false;
        } else {
          return true;
        }
      }
    }
    return true;
  }

  protected void readRowFromStream() {
    input.nextRow();
    short nullLen = input.readShort();
    BitSet nullBitSet = allNonNull;
    if (nullLen > 0) {
      nullBitSet = BitSet.valueOf(input.readBytes(nullLen));
    }
    int colCount = 0;
    // primitive type dimension
    for (; colCount < isNoDictColumn.length; colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        if (isNoDictColumn[colCount]) {
          int v = input.readShort();
          if (isRequired[colCount]) {
            byte[] b = input.readBytes(v);
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = b;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = DataTypeUtil
                  .getDataBasedOnDataTypeForNoDictionaryColumn(b,
                      storageColumns[colCount].getDataType(), true,
                      dataTypeConverter);
            }
          } else {
            input.skipBytes(v);
          }
        } else if (null != directDictionaryGenerators[colCount]) {
          if (isRequired[colCount]) {
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = input.copy(4);
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] =
                  directDictionaryGenerators[colCount].getValueFromSurrogate(input.readInt());
            } else {
              input.skipBytes(4);
            }
          } else {
            input.skipBytes(4);
          }
        } else {
          if (isRequired[colCount]) {
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = input.copy(4);
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = input.readInt();
            } else {
              input.skipBytes(4);
            }
          } else {
            input.skipBytes(4);
          }
        }
      }
    }
    // complex type dimension
    for (; colCount < dimensionCount; colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = null;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        short v = input.readShort();
        if (isRequired[colCount]) {
          byte[] b = input.readBytes(v);
          if (isFilterRequired[colCount]) {
            filterValues[filterMap[colCount]] = b;
          }
          if (isProjectionRequired[colCount]) {
            outputValues[projectionMap[colCount]] =
                queryTypes[colCount].getDataBasedOnDataType(ByteBuffer.wrap(b));
          }
        } else {
          input.skipBytes(v);
        }
      }
    }
    // measure
    DataType dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, colCount++) {
      if (nullBitSet.get(colCount)) {
        if (isFilterRequired[colCount]) {
          filterValues[filterMap[colCount]] = null;
        }
        if (isProjectionRequired[colCount]) {
          outputValues[projectionMap[colCount]] = null;
        }
      } else {
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN) {
          if (isRequired[colCount]) {
            boolean v = input.readBoolean();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(1);
          }
        } else if (dataType == DataTypes.SHORT) {
          if (isRequired[colCount]) {
            short v = input.readShort();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(2);
          }
        } else if (dataType == DataTypes.INT) {
          if (isRequired[colCount]) {
            int v = input.readInt();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(4);
          }
        } else if (dataType == DataTypes.LONG) {
          if (isRequired[colCount]) {
            long v = input.readLong();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(8);
          }
        } else if (dataType == DataTypes.DOUBLE) {
          if (isRequired[colCount]) {
            double v = input.readDouble();
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {
              outputValues[projectionMap[colCount]] = v;
            }
          } else {
            input.skipBytes(8);
          }
        } else if (DataTypes.isDecimal(dataType)) {
          int len = input.readShort();
          if (isRequired[colCount]) {
            BigDecimal v = DataTypeUtil.byteToBigDecimal(input.readBytes(len));
            if (isFilterRequired[colCount]) {
              filterValues[filterMap[colCount]] = v;
            }
            if (isProjectionRequired[colCount]) {

              outputValues[projectionMap[colCount]] =
                  dataTypeConverter.convertFromBigDecimalToDecimal(v);
            }
          } else {
            input.skipBytes(len);
          }
        }
      }
    }
  }

  private void readRawRowFromStream() {
    input.nextRow();
    short nullLen = input.readShort();
    BitSet nullBitSet = allNonNull;
    if (nullLen > 0) {
      nullBitSet = BitSet.valueOf(input.readBytes(nullLen));
    }
    int colCount = 0;
    // primitive type dimension
    for (; colCount < isNoDictColumn.length; colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      } else {
        if (isNoDictColumn[colCount]) {
          int v = input.readShort();
          outputValues[colCount] = input.readBytes(v);
        } else {
          outputValues[colCount] = input.readInt();
        }
      }
    }
    // complex type dimension
    for (; colCount < dimensionCount; colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = null;
      } else {
        short v = input.readShort();
        outputValues[colCount] = input.readBytes(v);
      }
    }
    // measure
    DataType dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, colCount++) {
      if (nullBitSet.get(colCount)) {
        outputValues[colCount] = null;
      } else {
        dataType = measureDataTypes[msrCount];
        if (dataType == DataTypes.BOOLEAN) {
          outputValues[colCount] = input.readBoolean();
        } else if (dataType == DataTypes.SHORT) {
          outputValues[colCount] = input.readShort();
        } else if (dataType == DataTypes.INT) {
          outputValues[colCount] = input.readInt();
        } else if (dataType == DataTypes.LONG) {
          outputValues[colCount] = input.readLong();
        } else if (dataType == DataTypes.DOUBLE) {
          outputValues[colCount] = input.readDouble();
        } else if (DataTypes.isDecimal(dataType)) {
          int len = input.readShort();
          outputValues[colCount] = DataTypeUtil.byteToBigDecimal(input.readBytes(len));
        }
      }
    }
  }

  public void close() {
    if (null != input) {
      input.close();
    }
  }
}
