package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.carbondata.core.datastore.impl.FileFactory;

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

public class TestHbaseWithCarbon {
  private static String quorum = "localhost";
  private static String port="2181";

  public static void main(String[] args) throws IOException {
    String path = "hdfs://localhost:9000/carbon-store/hbase-emp1";
    if (args.length > 3) {
      System.out.println("Args>>>>>>>>>>>> " + Arrays.toString(args));
      quorum = args[0];
      port = args[1];
      if (args[2].equalsIgnoreCase("create")) {
        path = args[3];
        boolean createCarbon = true;
        if (args.length > 4) {
          createCarbon = Boolean.parseBoolean(args[4]);
        }
        createTable(path, createCarbon);
      } else if (args[2].equalsIgnoreCase("drop")) {
        path = args[3];
        dropTable(path);
      } else if (args[2].equalsIgnoreCase("load")) {
        if (args.length > 4) {
          loadData(Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        } else {
          loadData(10*1000*1000, 1000);
        }
      }
    } else {
      System.out.println("Not enough argumets");
      System.out.println(" First 2 args must be hbase.zookeeper.quorum, hbase.zookeeper.property.clientPort");
      System.out.println(" Next args should be either create or drop for creating or dropping the table along with path");

    }

  }


  private static void loadData(int size, int batchSize) throws IOException {
    Connection connection = getConnection();
    System.out.println("Loading data with size "+ size +" and batch size "+ batchSize);
    Table emp = connection.getTable(TableName.valueOf("emp"));
    long l = System.currentTimeMillis();
    int k =0;
    Random random = new Random();
    List<Put> puts  = new ArrayList<>(batchSize);
    for (int i = 0; i < size; i++) {
      Put put = new Put(Bytes.toBytes(k + System.currentTimeMillis()));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("ravi"+(k%100000)));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("dept"), Bytes.toBytes("dept"+(k%1000)));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes("city"+(k%500)));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes((short) random.nextInt(60)));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("salary"), Bytes.toBytes(k * 2.1D));
      puts.add(put);
      k++;
      if (k % batchSize == 0) {
        emp.put(puts);
        puts = new ArrayList<>(batchSize);
      }
    }
    System.out.println("Time to insert : " + (System.currentTimeMillis() - l));
    emp.close();
    connection.close();
  }

  private static void createTable(String path, boolean createCarbon) throws IOException {
    Connection connection = getConnection();

    Admin admin = connection.getAdmin();
    // Instantiating table descriptor class
    HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf("emp"));

    // Adding column families to table descriptor
    tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
    if (createCarbon) {
      String schema =
          "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\",\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\",\"hbase_mapping\":\"key=ID,cf1:name=name,cf1:dept=dept,cf1:city=city,cf1:age=age,timestamp=timestamp,cf1:salary=salary\",\"path\":\""
              + path + "\"}}";
      System.out.println("Schema >>>>>>>>> " + schema);
      tableDescriptor.setValue("CARBON_SCHEMA", schema);
    }
    // Execute the table through admin
    admin.createTable(tableDescriptor);
    admin.enableTableReplication(TableName.valueOf("emp"));
    System.out.println(" Table created ");
    admin.close();
    connection.close();
  }

  private static void dropTable(String path) throws IOException {

    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(
        path));
    Connection connection = getConnection();
    Admin admin = connection.getAdmin();
    admin.disableTable(TableName.valueOf("emp"));
    admin.deleteTable(TableName.valueOf("emp"));
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
