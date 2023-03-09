package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 单DataSet多索引测试
 *
 * @author Haocheng Wang
 * Created on 2023/2/10
 */
public class DataBaseTest02 {

  static String DATASET_NAME = "database_test_06";
  static DataSetMeta DATASET_META;
  static Database INSTANCE;

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      INSTANCE = Database.getInstance();
      List<IndexMeta> list = new LinkedList<>();
      list.add(new IndexMeta(true, new XZ2IndexStrategy(), DATASET_NAME, "default"));
      list.add(new IndexMeta(false, new IDTIndexStrategy(), DATASET_NAME, "default"));
      DATASET_META = new DataSetMeta(DATASET_NAME, list);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void createDataSetTest() throws IOException {
    Database instance = Database.getInstance();
    instance.initDataBase();
    instance.createDataSet(DATASET_META);
    instance.closeConnection();
  }

  @Test
  public void dataSetExistsTest() throws IOException {
    Database instance = Database.getInstance();
    assert instance.dataSetExists(DATASET_NAME);
  }

  @Test
  public void listTableNamesTest() throws IOException {
    Database instance = Database.getInstance();
    TableName[] tableNames = instance.getAdmin().listTableNames();
    for (TableName tn : tableNames) {
      System.out.println(tn.getNameAsString());
    }
    instance.closeConnection();
  }

  @Test
  public void getDataSetMetaTest() throws IOException {
    Database instance = Database.getInstance();
    DataSetMeta meta = instance.getDataSetMeta(DATASET_NAME);
    System.out.println(meta);
    instance.closeConnection();
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}
