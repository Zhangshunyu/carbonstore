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

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.format.BlockletHeader;

/**
 * Stream vector/row record reader
 */
public class CarbonStreamRecordReader extends StreamRecordReader {

  // vector reader
  protected boolean isVectorReader;

  public CarbonStreamRecordReader(BlockExecutionInfo executionInfo, boolean isVectorReader,
      boolean useRawRow) {
    super(executionInfo, useRawRow);
    this.isVectorReader = isVectorReader;
    if (isVectorReader) {
      dataTypeConverter = new DataTypeConverterImpl();
    }
  }

  public void next(CarbonColumnarBatch columnarBatch) throws IOException {
    nextColumnarBatch(columnarBatch);
  }

  @Override public boolean hasNext() {
    if (isVectorReader) {
      // move to the next blocklet
      try {
        if (isFirstRow) {
          isFirstRow = false;
          initializeAtFirstRow();
        }
        if (!notConsumed) {
          boolean nextBlocklet = input.nextBlocklet();
          if (nextBlocklet) {
            notConsumed = true;
          }
          return nextBlocklet;
        } else {
          return true;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return super.hasNext();
    }
  }

  /**
   * for vector reader, check next columnar batch
   */
  private boolean nextColumnarBatch(CarbonColumnarBatch columnarBatch) throws IOException {
    boolean scanMore = false;
    notConsumed = false;
    if (!input.hasData()) {
      return true;
    }
    do {
      // read blocklet header
      BlockletHeader header = input.readBlockletHeader();
      if (isScanRequired(header)) {
        scanMore = !scanBlockletAndFillVector(header, columnarBatch);
      } else {
        input.skipBlockletData(true);
        scanMore = true;
      }
    } while (scanMore && input.nextBlocklet());
    return true;
  }

  private boolean scanBlockletAndFillVector(BlockletHeader header,
      CarbonColumnarBatch columnarBatch) throws IOException {
    // if filter is null and output projection is empty, use the row number of blocklet header
    if (skipScanData) {
      int rowNums = header.getBlocklet_info().getNum_rows();
      columnarBatch.setActualSize(rowNums);
      input.skipBlockletData(true);
      return rowNums > 0;
    }

    input.readBlockletData(header);
    int rowNum = 0;
    if (null == filter) {
      while (input.hasNext()) {
        readRowFromStream();
        putRowToColumnBatch(rowNum++, columnarBatch);
      }
    } else {
      try {
        while (input.hasNext()) {
          readRowFromStream();
          if (filter.applyFilter(filterRow, blockExecutionInfo.getDataBlock().getSegmentProperties()
              .getLastDimensionColOrdinal())) {
            putRowToColumnBatch(rowNum++, columnarBatch);
          }
        }
      } catch (FilterUnsupportedException e) {
        throw new IOException("Failed to filter row in vector reader", e);
      }
    }
    columnarBatch.setActualSize(rowNum);
    return rowNum > 0;
  }

  private void putRowToColumnBatch(int rowId, CarbonColumnarBatch columnarBatch) {
    for (int i = 0; i < projection.length; i++) {
      Object value = outputValues[i];
      putRowToColumnBatch(rowId, value, columnarBatch.columnVectors[i]);
    }
  }

  public void putRowToColumnBatch(int rowId, Object value, CarbonColumnVector columnVector) {
    DataType t = columnVector.getType();
    if (null == value) {
      columnVector.putNull(rowId);
    } else {
      if (t == DataTypes.BOOLEAN) {
        columnVector.putBoolean(rowId, (boolean) value);
      } else if (t == DataTypes.BYTE) {
        columnVector.putByte(rowId, (byte) value);
      } else if (t == DataTypes.SHORT) {
        columnVector.putShort(rowId, (short) value);
      } else if (t == DataTypes.INT) {
        columnVector.putInt(rowId, (int) value);
      } else if (t == DataTypes.LONG) {
        columnVector.putLong(rowId, (long) value);
      } else if (t == DataTypes.FLOAT) {
        columnVector.putFloat(rowId, (float) value);
      } else if (t == DataTypes.DOUBLE) {
        columnVector.putDouble(rowId, (double) value);
      } else if (t == DataTypes.STRING) {
        String v = (String) value;
        columnVector.putByteArray(rowId, v.getBytes());
      } else if (t instanceof DecimalType) {
        BigDecimal d = (BigDecimal) value;
        DecimalType decimalType = (DecimalType) t;
        columnVector.putDecimal(rowId, d, decimalType.getPrecision());
      } else if (t == DataTypes.DATE) {
        columnVector.putInt(rowId, (int) value);
      } else if (t == DataTypes.TIMESTAMP) {
        columnVector.putLong(rowId, (long) value);
      }
    }
  }

}
