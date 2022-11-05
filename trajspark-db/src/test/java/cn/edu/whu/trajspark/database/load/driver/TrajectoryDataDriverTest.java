package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Test;

/**
 * @author Xu Qi
 * @since 2022/11/3
 */
class TrajectoryDataDriverTest extends TestCase {

  @Test
  public void testBulkLoad() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String inPath = this.getClass().getClassLoader().getResource("traj_json/formatTra.txt").getPath();
    String output = "hdfs://localhost:9000/csv/";
    String database_name = "TraData_test";
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2IndexStrategy(),
        database_name
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(database_name, list);
    TrajectoryDataDriver trajectoryDataDriver = new TrajectoryDataDriver();
    trajectoryDataDriver.setConf(conf);
    int status = trajectoryDataDriver.bulkLoad(inPath, output, dataSetMeta);
    System.exit(status);
  }
}