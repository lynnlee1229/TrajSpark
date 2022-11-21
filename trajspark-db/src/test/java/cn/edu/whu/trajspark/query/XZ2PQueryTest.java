package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatial.XZ2PlusIndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Haocheng Wang
 * Created on 2022/11/16
 */
public class XZ2PQueryTest {
  static String DATASET_NAME = "xz2p_intersect_query_test";
  static SpatialQueryCondition spatialQueryCondition;
  static String QUERY_WKT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
      spatialQueryCondition = new SpatialQueryCondition(envelope, SpatialQueryCondition.SpatialQueryType.INTERSECT);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2PlusIndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
  }

  @Test
  public void testIndexTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2PlusIndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory trajectory : trips) {
      System.out.println(list.get(0).getIndexStrategy().index(trajectory));
    }
  }

  @Test
  public void testExecuteQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    for (Trajectory result : results) {
      System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy().index(result));
    }
    assertEquals(13, spatialQuery.executeQuery().size());
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }


}
