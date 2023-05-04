package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.XZ2TIndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/19
 */
public class TextBulkloadTest {
  static DataSetMeta dataSetMeta;
  public static String DATABASE_NAME = "bulkLoadTest2";

  static {
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(true, new XZ2IndexStrategy(), DATABASE_NAME, "default"));
    list.add(new IndexMeta(false, new IDTIndexStrategy(), DATABASE_NAME, "default"));
    dataSetMeta = new DataSetMeta(DATABASE_NAME, list);
  }

  // 确保hbase-site.xml, core-site.xml, hdfs-site.xml在class path中。
  // hdfs dfs -put traj/hdfs_traj_example.txt /data/
  public static void main(String[] args) throws Exception {
//    String inPath = TextBulkloadTest.class.getResource("/traj/hdfs_traj_example.txt").toString();
    String inPath = "hdfs:///data/hdfs_traj_example.txt";
    String output = "hdfs:///tmp/trajspark";
    Database.getInstance().createDataSet(dataSetMeta);
    TextBulkLoadDriver trajectoryDataDriver = new TextBulkLoadDriver();
    trajectoryDataDriver.setConf(HBaseConfiguration.create());
    trajectoryDataDriver.datasetBulkLoad(Parser.class, inPath, output, dataSetMeta);
  }

  @Test
  public void testBulkLoadNewIndex() throws Exception {
//    String inPath = TextBulkloadTest.class.getResource("/traj/hdfs_traj_example.txt").toString();
    String inPath = "hdfs:///data/hdfs_traj_example.txt";
    String output = "hdfs:///tmp/trajspark/";
    IndexMeta indexMeta = new IndexMeta(true,new XZ2TIndexStrategy(), DATABASE_NAME, "default5");
    Database.getInstance().addIndexMeta(DATABASE_NAME, indexMeta);
    TextBulkLoadDriver trajectoryDataDriver = new TextBulkLoadDriver();
    Configuration conf = HBaseConfiguration.create();
    trajectoryDataDriver.setConf(conf);
    trajectoryDataDriver.mainIndexBulkLoad(Parser.class, inPath, output, indexMeta);
  }

  @Test
  public void testDeleteDataSet2() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATABASE_NAME);
  }

}
