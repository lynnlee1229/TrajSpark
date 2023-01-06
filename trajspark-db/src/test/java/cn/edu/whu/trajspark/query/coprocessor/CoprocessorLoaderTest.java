package cn.edu.whu.trajspark.query.coprocessor;

import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.addCoprocessor;
import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.deleteCoprocessor;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.index.time.TimeIndexStrategy;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Test;

/**
 * @author Xu Qi
 * @since 2022/11/25
 */
class CoprocessorLoaderTest extends TestCase {
  static String DATASET_NAME = "xz2_intersect_query_test";
  static TimeIndexStrategy timeIndexStrategy = new TimeIndexStrategy(new XZTCoding());

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        timeIndexStrategy,
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
    instance.closeConnection();
  }

  @Test
  public void testAddCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
    String className = STQueryEndPoint.class.getCanonicalName();
    String jarPath = "hdfs://localhost:9000/coprocessor/trajspark-db-1.0-SNAPSHOT.jar";
    addCoprocessor(conf, tableName, className, jarPath);
  }

  @Test
  void TestDeleteCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
    deleteCoprocessor(conf, tableName);
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}