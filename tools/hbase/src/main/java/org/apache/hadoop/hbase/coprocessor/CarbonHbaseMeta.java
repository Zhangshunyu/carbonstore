package org.apache.hadoop.hbase.coprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class CarbonHbaseMeta {

  private Schema schema;

  private Map<String, String> tblProperties;

  private Map<QualifierArray, Integer> schemaMapping;

  private int[] rowKeyMapping;

  private int timestampMap = -1;

  private QualifierArray temp = new QualifierArray();

  public CarbonHbaseMeta(Schema schema, Map<String, String> tblProperties) {
    this.schema = schema;
    this.tblProperties = tblProperties;
    createSchemaMapping();
  }

  private void createSchemaMapping() {
    String keyName = "key";
    String timestamp = "timestamp";
    schemaMapping = new HashMap<>();
    List<Integer> keyMapping = new ArrayList<>();
    String hbase_mapping = tblProperties.get("hbase_mapping");
    String[] split = hbase_mapping.split(",");
    for (String s : split) {
      String[] map = s.split("=");
      if (map.length < 2) {
        throw new UnsupportedOperationException("Hbase mapping is not right " + s);
      }
      String[] qualifiers = map[0].split(":");
      if (map[0].equalsIgnoreCase(keyName)) {
        keyMapping.add(getSchemaIndex(map[1]));
      } else if (map[0].equalsIgnoreCase(timestamp)) {
        timestampMap = getSchemaIndex(map[1]);
      } else {
        if (qualifiers.length < 2) {
          throw new UnsupportedOperationException(
              "Hbase mapping is not right, please make sure to provide column family and qualifier "
                  + map[0]);
        }
        int schemaIndex = getSchemaIndex(map[1]);
        byte[] cf = Bytes.toBytesBinary(qualifiers[0]);
        byte[] qual = Bytes.toBytesBinary(qualifiers[1]);
        schemaMapping.put(new QualifierArray(cf, 0, cf.length, qual, 0, qual.length), schemaIndex);
      }
    }
    rowKeyMapping = ArrayUtils.toPrimitive(keyMapping.toArray(new Integer[0]));
    if (timestampMap == -1) {
      throw new UnsupportedOperationException(
          "Time stamp mapping is mandatory for hbase, "
              + "please use timestamp in hbase_mapping carbon property inside schema");
    }
  }

  private int getSchemaIndex(String columnName) {
    for (int i = 0; i < schema.getFields().length; i++) {
      if (schema.getFields()[i].getFieldName().equalsIgnoreCase(columnName)) {
        return i;
      }
    }
    throw new RuntimeException("Schema column mapping is not right " + columnName);
  }

  public int getSchemaIndexOfColumn(byte[] cf, int cfOffset, int cfLen, byte[] qual, int qualOffset,
      int qualLen) {
    temp.set(cf, cfOffset, cfLen, qual, qualOffset, qualLen);
    Integer integer = schemaMapping.get(temp);
    if (integer == null) {
      return -1;
    } else {
      return integer;
    }
  }

  public int getKeyColumnIndex() {
    // TODO row key split needed to be supported.
    return rowKeyMapping[0];
  }

  public int getTimestampMapIndex() {
    return timestampMap;
  }

  public Schema getSchema() {
    return schema;
  }

  public Map<String, String> getTblProperties() {
    return tblProperties;
  }

  public String convertData(byte[] value, int offset, int len, int schemaIndex) {
    DataType dataType = schema.getFields()[schemaIndex].getDataType();
    int id = dataType.getId();
    if (id == DataTypes.BOOLEAN.getId()) {
      return String.valueOf(value[offset]!= (byte) 0);
    } else if (id == DataTypes.STRING.getId()) {
      return Bytes.toString(value, offset, len);
    } else if (id == DataTypes.INT.getId()) {
      return String.valueOf(Bytes.toInt(value, offset, len));
    } else if (id == DataTypes.SHORT.getId()) {
      return String.valueOf(Bytes.toShort(value, offset, len));
    } else if (id == DataTypes.LONG.getId()) {
      return String.valueOf(Bytes.toLong(value, offset, len));
    } else if (id == DataTypes.DOUBLE.getId()) {
      return String.valueOf(Bytes.toDouble(value, offset));
    } else if (DataTypes.isDecimal(dataType)) {
      return String.valueOf(Bytes.toBigDecimal(value, offset, len));
    } else if (id == DataTypes.DATE.getId()) {
      return String.valueOf(Bytes.toInt(value, offset, len));
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return String.valueOf(Bytes.toLong(value, offset, len));
    } else if (id == DataTypes.VARCHAR.getId()) {
      return String.valueOf(Bytes.toString(value, offset, len));
    } else if (id == DataTypes.FLOAT.getId()) {
      return String.valueOf(Bytes.toFloat(value, offset));
    } else if (id == DataTypes.BYTE.getId()) {
      return String.valueOf(value[offset]);
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }

}
