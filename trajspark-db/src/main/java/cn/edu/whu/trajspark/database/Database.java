package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.database.table.MetaTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static cn.edu.whu.trajspark.constant.DBConstants.*;

/**
 * @author Haocheng Wang
 * Created on 2022/10/11
 */
public final class Database {
  private static final Logger logger = LoggerFactory.getLogger(Database.class);

  private Connection connection;
  private Admin admin;
  private Configuration configuration;
  private MetaTable metaTable;

  private static Database instance = new Database();

  private Database() {
    configuration = HBaseConfiguration.create();
  }

  public static Database getInstance() {
    return instance;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Table getTable(String tableName) throws IOException {
    return connection.getTable(TableName.valueOf(tableName));
  }

  public boolean dataSetExists(String datasetName) throws IOException {
    return metaTable.dataSetExists(datasetName);
  }

  public void createDataSet(DataSetMeta dataSetMeta) throws IOException {
    String dataSetName = dataSetMeta.getDataSetName();
    if (dataSetExists(dataSetName)) {
      logger.warn("The dataset(name: {}) already exists.", dataSetName);
      return;
    }
    // put data into dataset meta table
    metaTable.putDataSet(dataSetMeta);
    // create data table of the data set.
    createTable(dataSetName + DATA_TABLE_SUFFIX, new String[]{"data"});
    logger.info("Dataset {} created.", dataSetName);
  }

  public void deleteDataSet(String dataSetName) throws IOException {
    if (!dataSetExists(dataSetName)) {
      logger.warn("The dataset(name: {}) doesn't exists.", dataSetName);
      return;
    }
    // delete data from dataset meta table
    metaTable.deleteDataSet(dataSetName);
    // delete data table of the data set.
    deleteTable(dataSetName + DATA_TABLE_SUFFIX);
    logger.info("Dataset {} deleted.", dataSetName);
  }

  public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
    return metaTable.getDataSetMeta(dataSetName);
  }

  public DataTable getDataTable(String dataSetName) throws IOException {
    return new DataTable(dataSetName);
  }

  /**
   * HBase connections
   */
  public void closeConnection() throws IOException {
    if (connection != null) {
      connection.close();
      connection = null;
    }
    if (admin != null) {
      admin.close();
      admin = null;
    }
  }

  public void openConnection() throws IOException {
    int threads = Runtime.getRuntime().availableProcessors() * 4;
    ExecutorService service = new ForkJoinPool(threads);
    connection = ConnectionFactory.createConnection(configuration, service);
    admin = connection.getAdmin();
    initDataBase();
    metaTable = new MetaTable(getTable(META_TABLE_NAME));
  }

  /**
   * Initiate dataset meta table, if already exists, skip.
   */
  public void initDataBase() throws IOException {
    // 创建DataSetMeta表
    createTable(META_TABLE_NAME, new String[]{META_TABLE_COLUMN_FAMILY});
  }


  /**
   * HBase table options
   */
  public void createTable(String tableName, String[] columnFamilies) throws IOException {
    if (!tableExists(tableName)) {
      HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
      for (String str : columnFamilies) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
        hTableDescriptor.addFamily(hColumnDescriptor);
      }
      admin.createTable(hTableDescriptor);
    }
  }

  private boolean tableExists(String tableName) {
    boolean res;
    try {
      res = admin.tableExists(TableName.valueOf(tableName));
    } catch (IOException exception) {
      res = false;
      logger.warn(exception.getMessage());
    }
    return res;
  }

  public Admin getAdmin() {
    return admin;
  }

  public Connection getConnection() {
    return connection;
  }

  public void deleteTable(String tableName) throws IOException {
    if (this.tableExists(tableName)) {
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } else {
      throw new IOException("Table does not exists.");
    }
  }
}
