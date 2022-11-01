package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/22
 */
public class DataBaseTest {

  static DataSetMeta DATASET_META;
  static String DATASET_NAME = "database_test";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2IndexStrategy(),
        DATASET_NAME
    ));
    DATASET_META = new DataSetMeta(DATASET_NAME, list);
  }

  @Test
  public void listTableNamesTest() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    TableName[] tableNames = instance.getAdmin().listTableNames();
    for (TableName tn : tableNames) {
      System.out.println(tn.getNameAsString());
    }
    instance.closeConnection();
  }


  @Test
  public void createDataSetTest() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.initDataBase();
    instance.createDataSet(DATASET_META);
    instance.closeConnection();
  }

  @Test
  public void dataSetExistsTest() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    assert instance.dataSetExists(DATASET_NAME);
  }

  @Test
  public void getDataSetMetaTest() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataSetMeta meta = instance.getDataSetMeta(DATASET_NAME);
    System.out.println(meta);
    instance.closeConnection();
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}
