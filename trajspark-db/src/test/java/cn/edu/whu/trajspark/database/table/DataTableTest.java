package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils.*;

/**
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class DataTableTest extends TestCase {

  static String DATASET_NAME = "dataset_test";
  static DataSetMeta DATASET_META;
  static Database instance;


  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    instance = Database.getInstance();
    try {
      instance.openConnection();
      System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
      List<IndexMeta> list = new LinkedList<>();
      list.add(new IndexMeta(
          true,
          new XZ2IndexStrategy(),
          DATASET_NAME
      ));
      DATASET_META = new DataSetMeta(DATASET_NAME, list);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testCreateDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.initDataBase();
    instance.createDataSet(DATASET_META);
    instance.closeConnection();
  }

  public void testPutTrajectory() throws IOException, URISyntaxException {
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
  }

  public void testGetTrajectory() throws IOException {
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    Scan scan = new Scan();
    scan.addColumn(COLUMN_FAMILY, MBR_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, OBJECT_ID_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, SIGNATURE_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER);
    scan.addColumn(COLUMN_FAMILY, PTR_QUALIFIER);

    ResultScanner resultScanner = dataTable.getTable().getScanner(scan);
    Result result;
    while ((result = resultScanner.next()) != null) {
      Trajectory trajectory = TrajectorySerdeUtils.getTrajectory(result);
      System.out.println(trajectory);
    }
  }

  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}