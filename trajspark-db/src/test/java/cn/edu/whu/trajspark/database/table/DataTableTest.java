package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class DataTableTest extends TestCase {

  static String DATASET_NAME = "xz2_dataset_test";
  static Database instance;


  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    instance = Database.getInstance();
    try {
      instance.openConnection();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testPutTrajectory() throws IOException {
    List<Trajectory> list = ExampleTrajectoryUtil.getTrajectoriesFromResources("/traj");
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : list) {
      dataTable.put(t);
    }
  }

  public void testGetTrajectory() throws IOException {
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    ResultScanner resultScanner = dataTable.getTable().getScanner(new Scan());
    Result result;
    while ((result = resultScanner.next()) != null) {
      Trajectory trajectory = TrajectorySerdeUtils.getTrajectory(result, dataTable);
      System.out.println(trajectory);
    }
  }
}