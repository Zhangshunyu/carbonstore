package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHbaseWithCarbon {
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
  private static final String HBASE_TABLE_NAME = "table_name";

  public static void main(String[] args) throws IOException {
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
    String tableName = map.get(HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = "emp";
    }
    if (map.get(ACTION).equalsIgnoreCase("create")) {
      boolean createCarbon = true;
      if (map.get(CREATE_CARBON) != null) {
        createCarbon = Boolean.parseBoolean(map.get(CREATE_CARBON));
      }
      createTable(path, createCarbon, tableName);
    } else if (map.get(ACTION).equalsIgnoreCase("drop")) {
      dropTable(path, tableName);
    } else if (map.get(ACTION).equalsIgnoreCase("load")) {
      String startRow = map.get(START_ROW);
      long start = -1;
      if (startRow != null) {
        start = Long.parseLong(startRow);
      }
      loadData(Integer.parseInt(map.get(LOAD_SIZE)), Integer.parseInt(map.get(BATCH_SIZE)), false, start, tableName);
    } else if (map.get(ACTION).equalsIgnoreCase("delete")) {
      String row = map.get(ROW);
      if (row != null) {
        updateData(row, false, tableName);
      } else {
        String startRow = map.get(START_ROW);
        long start = -1;
        if (startRow != null) {
          start = Long.parseLong(startRow);
        }
        loadData(Integer.parseInt(map.get(LOAD_SIZE)), Integer.parseInt(map.get(BATCH_SIZE)), true, start, tableName);
      }
    } else if (map.get(ACTION).equalsIgnoreCase("update")) {
      String row = map.get(ROW);
      updateData(row, true, tableName);
    }
  }

  private static void updateData(String row, boolean update, String tableName) throws IOException {
    Connection connection = getConnection();
    Table emp = connection.getTable(TableName.valueOf(tableName));
    long l = System.currentTimeMillis();
    String[] split = row.split(",");
    Map<String, String> map = new HashMap<>();
    for (String s : split) {
      String[] strings = s.split(":");
      if (strings.length > 1) {
        map.put(strings[0].toLowerCase(), strings[1].trim());
      } else {
        map.put(strings[0].toLowerCase(), "");
      }
    }
    if (update) {
      System.out.println("Updating data " + row);
      Put put = new Put(Bytes.toBytes(Long.parseLong(map.get("id"))));
      if (map.get("name") != null) {
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(map.get("name")));
      }
      if (map.get("dept") != null) {
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("dept"), Bytes.toBytes(map.get("dept")));
      }
      if (map.get("city") != null) {
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes(map.get("city")));
      }
      if (map.get("age") != null) {
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(Short.parseShort(map.get("age"))));
      }
      if (map.get("salary") != null) {
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("salary"), Bytes.toBytes(Double.parseDouble(map.get("salary"))));
      }
      emp.put(put);
    } else {
      System.out.println("Deleting data " + row);
      Delete delete = new Delete(Bytes.toBytes(Long.parseLong(map.get("id"))));
      if (map.get("name") != null) {
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
      }
      if (map.get("dept") != null) {
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("dept"));
      }
      if (map.get("city") != null) {
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"));
      }
      if (map.get("age") != null) {
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
      }
      if (map.get("salary") != null) {
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("salary"));
      }

      emp.delete(delete);
    }
    System.out.println("Time to update : " + (System.currentTimeMillis() - l));
    emp.close();
    connection.close();
  }

  private static void loadData(int size, int batchSize, boolean isDelete, long startRow, String tableName) throws IOException {
    Connection connection = getConnection();
    Table emp = connection.getTable(TableName.valueOf(tableName));
    long l = System.currentTimeMillis();
    int k =0;
    long start = 1553693589489L;
    if (startRow > 0) {
      start = startRow;
    }
    if (!isDelete) {
      System.out.println("Loading data with size "+ size +" and batch size "+ batchSize);
      List<Put> puts = new ArrayList<>(batchSize);
      for (int i = 0; i < size; i++) {
        Put put = new Put(Bytes.toBytes(k + start));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
            Bytes.toBytes("ravi" + (k % 100000)));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("dept"), Bytes.toBytes("dept1" + (k % 1000)));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes("city" + (k % 500)));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes((short) (k % 60)));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("salary"), Bytes.toBytes(k * 200D));
        puts.add(put);
        k++;
        if (k % batchSize == 0) {
          emp.put(puts);
          puts = new ArrayList<>(batchSize);
        }
      }
      if (puts.size() > 0) {
        emp.put(puts);
      }
    } else {
      System.out.println("Deleting data with size "+ size +" and batch size "+ batchSize);
      List<Delete> deletes = new ArrayList<>(batchSize);
      for (int i = 0; i < size; i++) {
        Delete delete = new Delete(Bytes.toBytes(k + start));
        deletes.add(delete);
        k++;
        if (k % batchSize == 0) {
          emp.delete(deletes);
          deletes = new ArrayList<>(batchSize);
        }
      }
      if (deletes.size() > 0) {
        emp.delete(deletes);
      }
    }
    System.out.println("Time to insert : " + (System.currentTimeMillis() - l));
    emp.close();
    connection.close();
  }

  private static void createTable(String path, boolean createCarbon, String tableName) throws IOException {
    Connection connection = getConnection();

    Admin admin = connection.getAdmin();
    // Instantiating table descriptor class
    HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf(tableName));

    // Adding column families to table descriptor
    tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
    if (createCarbon) {
      String schema =
          "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\",\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\",\"deletestatus\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID,timestamp\",\"hbase_mapping\":\"key=ID,cf1:name=name,cf1:dept=dept,cf1:city=city,cf1:age=age,timestamp=timestamp,deletestatus=deletestatus,cf1:salary=salary\",\"path\":\""
              + path + "\"}}";
      System.out.println("Schema >>>>>>>>> " + schema);
      tableDescriptor.setValue("CARBON_SCHEMA", schema);
    }
    // Execute the table through admin
    admin.createTable(tableDescriptor);
    admin.enableTableReplication(TableName.valueOf(tableName));
    System.out.println(" Table created ");
    admin.close();
    connection.close();
  }

  private static void dropTable(String path, String tableName) throws IOException {
    System.out.println("Table Dropped");
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(
        path));
    Connection connection = getConnection();
    Admin admin = connection.getAdmin();
    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));
    admin.close();
    connection.close();
  }


  private static Connection getConnection() throws IOException {
    // Instantiating configuration class
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", quorum);
    conf.set("hbase.zookeeper.property.clientPort", port);
    return ConnectionFactory.createConnection(conf);
  }
}
