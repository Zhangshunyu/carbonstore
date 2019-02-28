package org.apache.hadoop.hbase.carbon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  public static void main(String[] args) throws IOException {
    dropTable();
    createTable();
    loadData();
  }


  private static void loadData() throws IOException {
    Connection connection = getConnection();
    Table emp = connection.getTable(TableName.valueOf("emp"));
    long l = System.currentTimeMillis();
    int k =0;
    for (int i = 0; i < 10000; i++) {
      List<Put> puts  = new ArrayList<>();
      for (int j = 0; j < 1000; j++) {
        Put put = new Put(Bytes.toBytes(k));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("ravi"+k));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("salary"), Bytes.toBytes((double) k * 2.1));
        puts.add(put);
        k++;
      }
      emp.put(puts);
    }
    System.out.println("Time to insert : " + (System.currentTimeMillis() - l));
    emp.close();
    connection.close();
  }

  private static void createTable() throws IOException {
    Connection connection = getConnection();

    Admin admin = connection.getAdmin();
    // Instantiating table descriptor class
    HTableDescriptor tableDescriptor = new
        HTableDescriptor(TableName.valueOf("emp"));

    // Adding column families to table descriptor
    tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
    tableDescriptor.setValue("CARBON_SCHEMA", "{\"ID\":\"int\",\"name\":\"string\",\"salary\":\"double\",\"timestamp\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\",\"hbase_mapping\":\"key=ID,cf1:name=name,timestamp=timestamp,cf1:salary=salary\",\"path\":\"hdfs://localhost:9000/carbon-store/hbase-emp\"}}");
    // Execute the table through admin
    admin.createTable(tableDescriptor);
    admin.enableTableReplication(TableName.valueOf("emp"));
    System.out.println(" Table created ");
    admin.close();
    connection.close();
  }

  private static void dropTable() throws IOException {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile("hdfs://localhost:9000/carbon-store/hbase-emp"));
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
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    return ConnectionFactory.createConnection(conf);
  }
}
