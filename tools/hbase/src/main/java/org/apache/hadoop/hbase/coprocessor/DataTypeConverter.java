package org.apache.hadoop.hbase.coprocessor;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.sdk.file.Field;

public interface DataTypeConverter {

  void convertRowKey(byte[] key, int offset, int len, int[] mapping, Field[] fields, String[] row);

  String convert(byte[] value, int offset, int len, DataType dataType);

}
