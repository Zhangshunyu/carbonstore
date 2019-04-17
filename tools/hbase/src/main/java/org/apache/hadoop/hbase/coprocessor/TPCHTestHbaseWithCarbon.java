package org.apache.hadoop.hbase.coprocessor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator.MILLIS_PER_DAY;

public class TPCHTestHbaseWithCarbon {
  private static String quorum = "localhost";
  private static String port="2181";

  private static final String ACTION="action";
  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String PATH = "path";
  private static final String CREATE_CARBON = "create_carbon";
  private static final String LOAD_SIZE = "load_size";
  private static final String BATCH_SIZE = "batch_size";
  private static final String ROW = "row";
  private static final String START_ROW = "start_row";
  private static final String TABLE_NAMES = "table_names";
  private static final String INPUT_PATH = "input_path";
  private static final String ROW_PERCENT = "row_percent";

  public static void main(String[] args) throws IOException, ParseException {
    String path = "hdfs://localhost:9000/carbon-store/hbase-emp1";
    Map<String, String> map = new HashMap<>();
    for (String arg : args) {
      String[] split = arg.split("=");
      map.put(split[0].toLowerCase(), split[1]);
    }
    System.out.println("Args>>>>>>>>>>>> " + map.toString());
    quorum = map.get(HOST);
    port = map.get(PORT);
    path = map.get(PATH);
    String tableNames = map.get(TABLE_NAMES);

    if (map.get(ACTION).equalsIgnoreCase("create")) {
      boolean createCarbon = true;
      if (map.get(CREATE_CARBON) != null) {
        createCarbon = Boolean.parseBoolean(map.get(CREATE_CARBON));
      }
      createTable(path, createCarbon);
    } else if (map.get(ACTION).equalsIgnoreCase("drop")) {
      dropTable(path);
    } else if (map.get(ACTION).equalsIgnoreCase("load")) {
      String inputPath = map.get(INPUT_PATH);
      loadData( Integer.parseInt(map.get(BATCH_SIZE)), inputPath, path, tableNames, false, 0);
    } else if (map.get(ACTION).equalsIgnoreCase("delete")) {
      String row = map.get(ROW);
      if (row != null) {
//        updateData(row, false, tableName);
      } else {
        String inputPath = map.get(INPUT_PATH);
        loadData( Integer.parseInt(map.get(BATCH_SIZE)), inputPath, path, tableNames, false, 0);
      }
    } else if (map.get(ACTION).equalsIgnoreCase("update")) {
      String inputPath = map.get(INPUT_PATH);
      String percent = map.get(ROW_PERCENT);
      if (percent == null) {
        percent = "10";
      }
      loadData( Integer.parseInt(map.get(BATCH_SIZE)), inputPath, path, tableNames, true, Integer.parseInt(percent));
    }
  }


  private static void loadData(int batchSize, String inputPath, String tablePath, String tableNames, boolean isUpdate, int recordNumber)
      throws IOException, ParseException {
    Connection connection = getConnection();
    String[] tablesToLoad = null;
    if (tableNames != null) {
      tablesToLoad = tableNames.split(",");
    }
    File file = new File(inputPath);

    File[] folders = file.listFiles();

    Map<String, String> statements = getCreateStatements(tablePath);

    for (String table : statements.keySet()) {

      if (tablesToLoad != null) {
        boolean found = false;
        for (String s : tablesToLoad) {
          if (s.equalsIgnoreCase(table)) {
            found = true;
          }
        }
        if (!found) {
          continue;
        }
      }

      File folderToLoad = null;
      for (int i = 0; i < folders.length; i++) {
        if (folders[i].getName().equalsIgnoreCase(table)) {
          folderToLoad = folders[i];
          break;
        }
      }
      if (folderToLoad == null) {
        throw new RuntimeException("Folder for table "+ table +" not present");
      }

      TableInfo tableInfo =
          SchemaReader.getTableInfo(AbsoluteTableIdentifier.from(tablePath + "/" + table));
      String primaryKeyCols = tableInfo.getFactTable().getTableProperties()
          .get(CarbonCommonConstants.PRIMARY_KEY_COLUMNS);
      List<ColumnSchemaHolder> orderedColumns = new ArrayList<>();
      Table emp = connection.getTable(TableName.valueOf(table));
      long l = System.currentTimeMillis();
      int k =0;
      for (ColumnSchema schema : tableInfo.getFactTable().getListOfColumns()) {
        if (!(schema.getColumnName().equalsIgnoreCase("timestamp") || schema.getColumnName().equalsIgnoreCase("deletestatus"))) {
          orderedColumns.add(new ColumnSchemaHolder(schema));
        }
      }
      Collections.sort(orderedColumns);
      String[] primaryCols = primaryKeyCols.split(",");
      int[] rowIndexes = new int[primaryCols.length];
      for (int i = 0; i < orderedColumns.size(); i++) {
        for (int j = 0; j < primaryCols.length; j++) {
          if (orderedColumns.get(i).columnSchema.getColumnName().equalsIgnoreCase(primaryCols[j])) {
            rowIndexes[j] = i;
            break;
          }
        }
      }
      System.out.println("Loading : "+table );
      byte[] family = Bytes.toBytes("c1");
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      File[] files = folderToLoad.listFiles(new FileFilter() {
        @Override public boolean accept(File pathname) {
          return pathname.getName().endsWith(".tbl");
        }
      });
      for (File fileTolOad : files) {

        BufferedReader fileReader = new BufferedReader(new FileReader(fileTolOad));
        String line = fileReader.readLine();
        List<Put> puts = new ArrayList<>(batchSize);
        if (!isUpdate) {
          System.out.println("Started Loading file : "+fileTolOad.getAbsolutePath() );
          while (line != null) {
            String[] cols = line.split("\\|");
            byte[] rowKey = generateRowKey(rowIndexes, cols, orderedColumns, stream);
            stream.reset();
            Put put = new Put(rowKey);
            int i = 0;
            for (ColumnSchemaHolder column : orderedColumns) {
              if (!column.columnSchema.isPrimaryKeyColumn()) {
                put.addColumn(family, Bytes.toBytes(column.columnSchema.getColumnName().toLowerCase()),
                    convert(cols[i], column.columnSchema.getDataType()));
              }
              i++;
            }
            puts.add(put);
            k++;
            if (k % batchSize == 0) {
              emp.put(puts);
              puts = new ArrayList<>(batchSize);
            }
            line = fileReader.readLine();
          }
        } else {
          System.out.println("Started updating file : "+fileTolOad.getAbsolutePath() );
          int ln = 1;
          BufferedWriter writer = new BufferedWriter(new FileWriter(fileTolOad.getParentFile().getAbsolutePath()+"/"+fileTolOad.getName()+"updated"));
          while (line != null) {
            if (ln % batchSize == 0) {
              writer.write(line);
              writer.newLine();
              String[] cols = line.split("\\|");
              byte[] rowKey = generateRowKey(rowIndexes, cols, orderedColumns, stream);
              stream.reset();
              Put put = new Put(rowKey);
              int i = 0;
              for (ColumnSchemaHolder column : orderedColumns) {
                if (column.columnSchema.getColumnName().equalsIgnoreCase("l_linestatus")) {
                  put.addColumn(family, Bytes.toBytes(column.columnSchema.getColumnName().toLowerCase()),
                      convert("Z", column.columnSchema.getDataType()));
                }
                i++;
              }
              puts.add(put);
            }
            ln++;
            if (puts.size() >= batchSize) {
              emp.put(puts);
              puts = new ArrayList<>(batchSize);
            }
            line = fileReader.readLine();
          }
          writer.close();
        }
        fileReader.close();
        if (puts.size() > 0) {
          emp.put(puts);
        }
      }
      System.out.println("Time to insert : "+table +" : " + (System.currentTimeMillis() - l));
      emp.close();
    }
    connection.close();
  }

  private static byte[] generateRowKey(int[] rowIndexes, String[] data, List<ColumnSchemaHolder> orderedColumns, ByteArrayOutputStream stream)
      throws IOException, ParseException {
    for (int i = 0; i < rowIndexes.length; i++) {
      ColumnSchemaHolder schemaHolder = orderedColumns.get(rowIndexes[i]);
      convert(data[rowIndexes[i]], schemaHolder.columnSchema.getDataType(), stream);
    }
    return stream.toByteArray();
  }

  private static SimpleDateFormat dateFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
  private static SimpleDateFormat timeFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);

  public static void convert(String value, DataType dataType, ByteArrayOutputStream stream)
      throws IOException, ParseException {
    int id = dataType.getId();
    if (id == DataTypes.BOOLEAN.getId()) {
      stream.write(Bytes.toBytes(Boolean.parseBoolean(value)));
    } else if (id == DataTypes.STRING.getId()) {
      byte[] bytes = Bytes.toBytes(value);
      stream.write(Bytes.toBytes(bytes.length));
      stream.write(bytes);
    } else if (id == DataTypes.INT.getId()) {
      stream.write(Bytes.toBytes(Integer.parseInt(value)));
    } else if (id == DataTypes.SHORT.getId()) {
      stream.write(Bytes.toBytes(Short.parseShort(value)));
    } else if (id == DataTypes.LONG.getId()) {
      stream.write(Bytes.toBytes(Long.parseLong(value)));
    } else if (id == DataTypes.DOUBLE.getId()) {
      stream.write(Bytes.toBytes(Double.parseDouble(value)));
    } else if (DataTypes.isDecimal(dataType)) {
      stream.write(Bytes.toBytes(new BigDecimal(value)));
    } else if (id == DataTypes.DATE.getId()) {
      stream.write(Bytes.toBytes( (int)(dateFormat.parse(value).getTime()/MILLIS_PER_DAY)));
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      stream.write(Bytes.toBytes(timeFormat.parse(value).getTime()));
    } else if (id == DataTypes.VARCHAR.getId()) {
      byte[] bytes = Bytes.toBytes(value);
      stream.write(Bytes.toBytes(bytes.length));
      stream.write(bytes);
    } else if (id == DataTypes.FLOAT.getId()) {
      stream.write(Bytes.toBytes(Float.parseFloat(value)));
    } else if (id == DataTypes.BYTE.getId()) {
      stream.write(Byte.parseByte(value));
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }

  public static byte[] convert(String value, DataType dataType)
      throws IOException, ParseException {
    int id = dataType.getId();
    if (id == DataTypes.BOOLEAN.getId()) {
      return Bytes.toBytes(Boolean.parseBoolean(value));
    } else if (id == DataTypes.STRING.getId()) {
      return Bytes.toBytes(value);
    } else if (id == DataTypes.INT.getId()) {
      return Bytes.toBytes(Integer.parseInt(value));
    } else if (id == DataTypes.SHORT.getId()) {
      return Bytes.toBytes(Short.parseShort(value));
    } else if (id == DataTypes.LONG.getId()) {
      return Bytes.toBytes(Long.parseLong(value));
    } else if (id == DataTypes.DOUBLE.getId()) {
      return Bytes.toBytes(Double.parseDouble(value));
    } else if (DataTypes.isDecimal(dataType)) {
      return Bytes.toBytes(new BigDecimal(value));
    } else if (id == DataTypes.DATE.getId()) {
      return Bytes.toBytes((int) (dateFormat.parse(value).getTime()/MILLIS_PER_DAY));
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return Bytes.toBytes(timeFormat.parse(value).getTime());
    } else if (id == DataTypes.VARCHAR.getId()) {
      return Bytes.toBytes(value);
    } else if (id == DataTypes.FLOAT.getId()) {
      return Bytes.toBytes(Float.parseFloat(value));
    } else if (id == DataTypes.BYTE.getId()) {
      return new byte[] {Byte.parseByte(value)};
    } else {
      throw new UnsupportedOperationException(
          "Provided datatype " + dataType + " is not supported");
    }
  }


  private void updateTpchRecords() {

  }

  private static void createTable(String path, boolean createCarbon) throws IOException {
    Connection connection = getConnection();

    Admin admin = connection.getAdmin();

    Map<String, String> map = getCreateStatements(path);
    for (Map.Entry<String, String> entry : map.entrySet()) {
      // Instantiating table descriptor class
      HTableDescriptor tableDescriptor = new
          HTableDescriptor(TableName.valueOf(entry.getKey().toLowerCase()));

      // Adding column families to table descriptor
      tableDescriptor.addFamily(new HColumnDescriptor("c1"));
      if (createCarbon) {
        String schema = entry.getValue();
        System.out.println("Schema >>>>>>>>> " + schema);
        tableDescriptor.setValue("CARBON_SCHEMA", schema);
      }
      // Execute the table through admin
      admin.createTable(tableDescriptor);
      admin.enableTableReplication(TableName.valueOf(entry.getKey().toLowerCase()));
      System.out.println(" Table created "+ entry.getKey().toLowerCase());
    }
    admin.close();
    connection.close();
  }

  private static void dropTable(String path) throws IOException {
    Map<String, String> map = getCreateStatements(path);
    Connection connection = getConnection()                 ;
    Admin admin = connection.getAdmin();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      System.out.println("Table Dropped");
      admin.disableTable(TableName.valueOf(entry.getKey().toLowerCase()));
      admin.deleteTable(TableName.valueOf(entry.getKey().toLowerCase()));
    }
    admin.close();
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(path));
    connection.close();
  }


  private static Connection getConnection() throws IOException {
    // Instantiating configuration class
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", quorum);
    conf.set("hbase.zookeeper.property.clientPort", port);
    return ConnectionFactory.createConnection(conf);
  }

  private static Map<String, String> getCreateStatements(String path) {
    Map<String, String> qs = new LinkedHashMap<>();
    qs.put("lineitem", "{\"L_ORDERKEY\":\"BIGINT\",\"L_PARTKEY\":\"BIGINT\",\"L_SUPPKEY\":\"BIGINT\",\"L_LINENUMBER\":\"INT\",\"L_QUANTITY\":\"double\",\"L_EXTENDEDPRICE\":\"double\",\"L_DISCOUNT\":\"double\",\"L_TAX\":\"double\",\"L_RETURNFLAG\":\"string\",\"L_LINESTATUS\":\"string\",\"L_SHIPDATE\":\"date\",\"L_COMMITDATE\":\"date\",\"L_RECEIPTDATE\":\"date\",\"L_SHIPINSTRUCT\":\"string\",\"L_SHIPMODE\":\"string\",\"L_COMMENT\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=L_ORDERKEY,key=L_LINENUMBER,c1:L_PARTKEY=L_PARTKEY,c1:L_SUPPKEY=L_SUPPKEY,c1:L_QUANTITY=L_QUANTITY,c1:L_EXTENDEDPRICE=L_EXTENDEDPRICE,c1:L_DISCOUNT=L_DISCOUNT,c1:L_TAX=L_TAX,c1:L_RETURNFLAG=L_RETURNFLAG,c1:L_LINESTATUS=L_LINESTATUS,c1:L_SHIPDATE=L_SHIPDATE,c1:L_COMMITDATE=L_COMMITDATE,c1:L_RECEIPTDATE=L_RECEIPTDATE,c1:L_SHIPINSTRUCT=L_SHIPINSTRUCT,c1:L_SHIPMODE=L_SHIPMODE,c1:L_COMMENT=L_COMMENT,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/lineitem" + "\"}}");
    qs.put("customer", "{\"c_custkey\":\"bigint\",\"c_name\":\"string\",\"c_address\":\"string\",\"c_nationkey\":\"int\",\"c_phone\":\"string\",\"c_acctbal\":\"double\",\"c_mktsegment\":\"string\",\"c_comment\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=c_custkey,c1:c_name=c_name,c1:c_address=c_address,c1:c_nationkey=c_nationkey,c1:c_phone=c_phone,c1:c_acctbal=c_acctbal,c1:c_mktsegment=c_mktsegment,c1:c_comment=c_comment,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/customer" + "\"}}");
    qs.put("nation", "{\"n_nationkey\":\"int\",\"n_name\":\"string\",\"n_regionkey\":\"int\",\"n_comment\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=n_nationkey,c1:n_name=n_name,c1:n_regionkey=n_regionkey,c1:n_comment=n_comment,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/nation" + "\"}}");
    qs.put("orders", "{\"o_orderkey\":\"bigint\",\"o_custkey\":\"bigint\",\"o_orderstatus\":\"string\",\"o_totalprice\":\"double\",\"o_orderdate\":\"date\",\"o_orderpriority\":\"string\",\"o_clerk\":\"string\",\"o_shippriority\":\"int\",\"o_comment\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=o_orderkey,c1:o_custkey=o_custkey,c1:o_orderstatus=o_orderstatus,c1:o_totalprice=o_totalprice,c1:o_orderdate=o_orderdate,c1:o_orderpriority=o_orderpriority,c1:o_clerk=o_clerk,c1:o_shippriority=o_shippriority,c1:o_comment=o_comment,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/orders" + "\"}}");
    qs.put("part", "{\"p_partkey\":\"bigint\",\"p_name\":\"STRING\",\"p_mfgr\":\"STRING\",\"p_brand\":\"STRING\",\"p_type\":\"string\",\"p_size\":\"int\",\"p_container\":\"string\",\"p_retailprice\":\"double\",\"p_comment\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=p_partkey,c1:p_name=p_name,c1:p_mfgr=p_mfgr,c1:p_brand=p_brand,c1:p_type=p_type,c1:p_size=p_size,c1:p_container=p_container,c1:p_retailprice=p_retailprice,c1:p_comment=p_comment,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/part" + "\"}}");
    qs.put("partsupp", "{\"PS_PARTKEY\":\"bigint\",\"PS_SUPPKEY\":\"BIGINT\",\"PS_AVAILQTY\":\"int\",\"PS_SUPPLYCOST\":\"double\",\"PS_COMMENT\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=PS_PARTKEY,key=PS_SUPPKEY,c1:PS_AVAILQTY=PS_AVAILQTY,c1:PS_SUPPLYCOST=PS_SUPPLYCOST,c1:PS_COMMENT=PS_COMMENT,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/partsupp" + "\"}}");
    qs.put("region", "{\"r_regionkey\":\"int\",\"r_name\":\"string\",\"r_comment\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=r_regionkey,c1:r_name=r_name,c1:r_comment=r_comment,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/region" + "\"}}");
    qs.put("supplier", "{\"S_SUPPKEY\":\"bigint\",\"S_NAME\":\"string\",\"S_ADDRESS\":\"string\",\"S_NATIONKEY\":\"int\",\"S_PHONE\":\"string\",\"S_ACCTBAL\":\"double\",\"S_COMMENT\":\"string\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\",\"BAD_RECORDS_ACTION\":\"FORCE\",\"hbase_mapping\":\"key=S_SUPPKEY,c1:S_COMMENT=S_COMMENT,c1:S_NAME=S_NAME,c1:S_ADDRESS=S_ADDRESS,c1:S_NATIONKEY=S_NATIONKEY,c1:S_PHONE=S_PHONE,c1:S_ACCTBAL=S_ACCTBAL,timestamp=timestamp,deletestatus=deletestatus\",\"path\":\""
        + path +"/supplier" + "\"}}");
    return qs;
  }


  private static class ColumnSchemaHolder implements Comparable<ColumnSchemaHolder> {

    ColumnSchema columnSchema;

    public ColumnSchemaHolder(ColumnSchema columnSchema) {
      this.columnSchema = columnSchema;
    }

    @Override public int compareTo(ColumnSchemaHolder o) {
      return Integer.valueOf(this.columnSchema.getSchemaOrdinal()).compareTo(o.columnSchema.getSchemaOrdinal());
    }
  }
}
