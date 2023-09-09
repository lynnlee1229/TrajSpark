package cn.edu.whu.trajspark.query.advanced;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.index.spatialtemporal.TXZ2IndexStrategy;
import cn.edu.whu.trajspark.query.IDTemporalQueryTest;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/17
 */
public class PKNNQueryTest {

  static String DATASET_NAME = "Knn_Query_Test";
  static Database instance;
  static double MAX_DISTANCE_KM = 10;
  static int K = 10;

  static {
    try {
      instance = Database.getInstance();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(true, new TXZ2IndexStrategy(), DATASET_NAME, "default"));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    System.out.println("to put " + trips.size() + " trajectories");
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
  }

  // 测试在TXZ2索引表上的KNN
  @Test
  public void testKNNQuery1() throws IOException {
    BasePoint basePoint = new BasePoint(114.05185384869783, 22.535191684309407);
    TemporalQueryCondition tqc = new TemporalQueryCondition(IDTemporalQueryTest.testTimeLine2, TemporalQueryType.CONTAIN);
    PKNNQuery pknnQuery = new PKNNQuery(instance.getDataSet(DATASET_NAME), 10, basePoint, tqc, 10);
    List<Trajectory> actual = pknnQuery.execute();
    List<Trajectory> expect = getAnswer(basePoint, tqc);
    System.out.println("------ actual ------");
    for (Trajectory t : actual) {
      System.out.println(t);
    }
    System.out.println("------ expect ------");
    for (Trajectory t : expect) {
      System.out.println(t);
    }
  }

  public List<Trajectory> getAnswer(BasePoint basePoint, TemporalQueryCondition tqc) {
    try {
      List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
          new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
      PKNNAnswer pknnAnswer = new PKNNAnswer(MAX_DISTANCE_KM, basePoint, tqc, K, trips);
      return pknnAnswer.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new LinkedList<>();
  }
}
