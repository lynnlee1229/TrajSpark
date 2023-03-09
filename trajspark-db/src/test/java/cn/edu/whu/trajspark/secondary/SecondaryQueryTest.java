package cn.edu.whu.trajspark.secondary;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.XZ2TIndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import cn.edu.whu.trajspark.query.IDTemporalQueryTest;
import cn.edu.whu.trajspark.query.SpatialQueryTest;
import cn.edu.whu.trajspark.query.basic.IDTemporalQuery;
import cn.edu.whu.trajspark.query.basic.SpatialTemporalQuery;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * @author Haocheng Wang
 * Created on 2023/2/14
 */
public class SecondaryQueryTest {
  static String DATASET_NAME = "secondary_query_test";

  // 查询条件
  static TemporalQueryCondition temporalContainQuery = IDTemporalQueryTest.temporalContainCondition;
  static TemporalQueryCondition temporalIntersectQuery = IDTemporalQueryTest.temporalIntersectCondition;
  static IDQueryCondition idQueryCondition = IDTemporalQueryTest.idQueryCondition;
  static SpatialQueryCondition spatialIntersectQueryCondition = SpatialQueryTest.spatialIntersectQueryCondition;
  static SpatialQueryCondition spatialContainedQueryCondition = SpatialQueryTest.spatialContainedQueryCondition;

  // index strategies
  static IDTIndexStrategy IDTIndexStrategy =  IDTemporalQueryTest.IDTIndexStrategy;
  static XZ2IndexStrategy xz2IndexStrategy = new XZ2IndexStrategy();
  static XZ2TIndexStrategy xz2TIndexStrategy = new XZ2TIndexStrategy();


  @Test
  public void createDataSet() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    // 创建数据集，3个索引：S, ID_T, TS，TS为主索引，S主索引
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(true, xz2IndexStrategy, DATASET_NAME, "default"));
    list.add(new IndexMeta(false, IDTIndexStrategy, DATASET_NAME, "default"));
    list.add(new IndexMeta(true, xz2TIndexStrategy, DATASET_NAME, "default"));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
  }

  @Test
  public void deleteDataSet() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }

  // 注册
  // alter 'secondary_query_test3-XZ2-default', 'coprocessor'=>'hdfs:///coprocessor/trajspark-db-1.0-SNAPSHOT.jar|cn.edu.whu.trajspark.secondary.SecondaryObserver||'
  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
  }

  @Test
  public void idTQuery() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, temporalIntersectQuery, idQueryCondition);
    List<Trajectory> trajectories = IDTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      trajectory.getTrajectoryFeatures();
      System.out.println(trajectory);
    }
    assertEquals(13, IDTemporalQuery.executeQuery().size());
  }

  @Test
  public void spatialTemporalQuery() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    SpatialTemporalQuery stQuery = new SpatialTemporalQuery(dataSet, new SpatialTemporalQueryCondition(spatialContainedQueryCondition, temporalContainQuery));
    List<Trajectory> trajectories = stQuery.executeQuery();
    for (Trajectory trajectory : trajectories) {
      System.out.println(trajectory);
    }
    assertEquals(8, trajectories.size());
  }

  @Test
  public void spatialTemporalQuery2() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    SpatialTemporalQuery stQuery = new SpatialTemporalQuery(dataSet, new SpatialTemporalQueryCondition(spatialIntersectQueryCondition, temporalIntersectQuery));
    List<Trajectory> trajectories = stQuery.executeQuery();
    for (Trajectory trajectory : trajectories) {
      System.out.println(trajectory);
    }
    assertEquals(1, trajectories.size());
  }

  @Test
  public void spatialQuery() throws IOException {

  }

}
