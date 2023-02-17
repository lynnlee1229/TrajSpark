package cn.edu.whu.trajspark.secondary;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/10
 */
public class PutTest {


  static String DATASET_NAME = "secondary_put_test2";
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
  public void createDataSet() throws IOException, URISyntaxException {
    INSTANCE.createDataSet(DATASET_META);
    INSTANCE.closeConnection();
  }

  @Test
  public void deleteDataSet() throws IOException, URISyntaxException {
    INSTANCE.deleteDataSet(DATASET_META.getDataSetName());
    INSTANCE.closeConnection();
  }


// alter 'secondary_put_test2-XZ2-default', 'coprocessor'=>'hdfs:///coprocessor/trajspark-db.jar|cn.edu.whu.trajspark.secondary.SecondaryObserver||'
  @Test
  public void deployObserverOnTable() throws IOException, URISyntaxException {

  }
  // alter 'secondary_put_test-XZ2-default', METHOD => 'table_att_unset', NAME => 'coprocessor$1'

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = INSTANCE.getDataSet(DATASET_NAME).getCoreIndexTable();
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
  }

  @Test
  public void testQuerySecondaryIndex() throws IOException, URISyntaxException {

  }
}
