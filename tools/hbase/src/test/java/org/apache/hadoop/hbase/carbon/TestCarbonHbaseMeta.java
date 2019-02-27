package org.apache.hadoop.hbase.carbon;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.sdk.file.CarbonSchemaWriter;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.hadoop.hbase.coprocessor.CarbonHbaseMeta;

public class TestCarbonHbaseMeta {


  static String schemaStr = "{\"ID\":\"string\", \"name\":\"string\",\"salary\":\"double\", \"timestamp\":\"timestamp\",\"tblproperties\": {\"sort_columns\":\"ID\", \"hbase_mapping\":\"key=ID,cf1:name=name,timestamp=timestamp,cf1:salary=salary\"}}";

  public static void main(String[] strings) throws IOException {

    Map<String, String > tblproperties = new HashMap<>();
    Schema schema = CarbonSchemaWriter.convertToSchemaFromJSON(schemaStr, tblproperties);

    System.out.println(tblproperties);

    CarbonHbaseMeta meta = new CarbonHbaseMeta(schema, tblproperties);
    System.out.println(meta.getKeyColumnIndex());
    System.out.println(meta.getTimestampMapIndex());
  }

}
