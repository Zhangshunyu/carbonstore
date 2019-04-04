package org.apache.carbondata.core.scan.primarykey;

import org.apache.carbondata.core.util.DataTypeConverterImpl;

public class PrimaryKeyDataTypeConverterImpl extends DataTypeConverterImpl {

  @Override public Object convertFromByteToUTF8String(byte[] data) {
    return data;
  }
}
