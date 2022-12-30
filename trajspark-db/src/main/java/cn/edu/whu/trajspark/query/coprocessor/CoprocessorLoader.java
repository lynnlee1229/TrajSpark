package cn.edu.whu.trajspark.query.coprocessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

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
      HTableDescriptor htd = new HTableDescriptor(tableName);
      HColumnDescriptor columnFamily1 = new HColumnDescriptor(familyName);
      htd.addFamily(columnFamily1);
      htd.addCoprocessor(className, new Path(jarPath),
          Coprocessor.PRIORITY_USER, null);
      admin.modifyTable(TableName.valueOf(tableName), htd);
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
      HTableDescriptor htd = new HTableDescriptor(tableName);
      HColumnDescriptor columnFamily1 = new HColumnDescriptor(familyName);
      htd.addFamily(columnFamily1);
      admin.modifyTable(TableName.valueOf(tableName), htd);
      admin.enableTable(TableName.valueOf(tableName));
      admin.close();
      connection.close();
    } else {
      throw new IOException("Table does not exists.");
    }
  }
}
