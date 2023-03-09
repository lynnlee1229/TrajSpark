package cn.edu.whu.trajspark.query.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

/**
 * need to new HTableDescriptor to modifyTable HTableDescriptor htd =
 * admin.getTableDescriptor(TableName.valueOf(Bytes.toBytes(tableName))); HTableDescriptor htd = new
 * HTableDescriptor(tableName);
 *
 * @author Xu Qi
 * @since 2022/11/13
 */
public class CoprocessorLoader {

  static String familyName = "data";

  public static void addCoprocessor(Configuration conf, String tableName, String className,
      String jarPath) throws IOException {
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    if (admin.tableExists(TableName.valueOf(tableName))) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.modifyTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).build());
      admin.enableTable(TableName.valueOf(tableName));
      admin.close();
      connection.close();
    } else {
      throw new IOException("Table does not exists.");
    }

  }

  public static void deleteCoprocessor(Configuration conf, String tableName) throws IOException {
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    if (admin.tableExists(TableName.valueOf(tableName))) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.modifyTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).build());
      admin.enableTable(TableName.valueOf(tableName));
      admin.close();
      connection.close();
    } else {
      throw new IOException("Table does not exists.");
    }
  }
}
