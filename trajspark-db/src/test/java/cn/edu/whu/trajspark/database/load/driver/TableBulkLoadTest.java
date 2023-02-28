package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/20
 */
public class TableBulkLoadTest {

  // 在已有Dataset的基础之上再添加索引
  // 确保hbase-site.xml, core-site.xml, hdfs-site.xml在class path中。
  @Test
  public void testBulkLoad() throws Exception {
    String output = "hdfs:///tmp/trajspark";
    DataSet dataSet = Database.getInstance().getDataSet(TextBulkloadTest.DATABASE_NAME);
    Configuration conf = HBaseConfiguration.create();
    IndexMeta coreIndexMeta = dataSet.getCoreIndexTable().getIndexMeta();
    IndexMeta newIndexMeta = new IndexMeta(false, new XZ2IndexStrategy(), TextBulkloadTest.DATABASE_NAME, "additional_index2");
    Database.getInstance().addIndexMeta(newIndexMeta.getDataSetName(), newIndexMeta);
    DataSetMeta dataSetMeta = Database.getInstance().getDataSetMeta(newIndexMeta.getDataSetName());
    TableBulkLoadDriver tableBulkLoadDriver = new TableBulkLoadDriver();
    tableBulkLoadDriver.setConf(conf);
    tableBulkLoadDriver.bulkLoad(output, newIndexMeta, dataSetMeta);
  }

  @Test
  public void testDeleteDataSet2() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet("bulkLoadTest3");
  }

}
