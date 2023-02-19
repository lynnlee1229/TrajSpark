package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.*;
import static cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils.mainRowToTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class DataTableTest extends TestCase {

  static String DATASET_NAME = "dataset_test";
  static DataSetMeta DATASET_META;
  static Database instance;
  static IndexTable INDEX_TABLE;


  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      instance = Database.getInstance();
      System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
      List<IndexMeta> list = new LinkedList<>();
      list.add(new IndexMeta(
          true,
          new XZ2IndexStrategy(),
          DATASET_NAME,
          "default"
      ));
      DATASET_META = new DataSetMeta(DATASET_NAME, list);
      INDEX_TABLE = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testCreateDataSet() throws IOException {
    instance.createDataSet(DATASET_META);
  }

  public void testPutTrajectory() throws IOException, URISyntaxException {
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    for (Trajectory t : trips) {
      INDEX_TABLE.putForMainTable(t);
    }
  }

  public void testGetTrajectory() throws IOException {
    Scan scan = new Scan();
    scan.addColumn(COLUMN_FAMILY, MBR_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, OBJECT_ID_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, SIGNATURE_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, PTR_QUALIFIER);

    ResultScanner resultScanner = INDEX_TABLE.getTable().getScanner(scan);
    Result result;
    while ((result = resultScanner.next()) != null) {
      Trajectory trajectory = mainRowToTrajectory(result);
      System.out.println(trajectory);
    }
  }

  public void testDeleteDataSet() throws IOException {
    instance.deleteDataSet(DATASET_NAME);
  }
}