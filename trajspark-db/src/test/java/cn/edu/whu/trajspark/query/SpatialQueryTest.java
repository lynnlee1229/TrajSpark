package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Xu Qi
 * @since 2022/12/26
 */
class SpatialQueryTest extends TestCase {

  static String DATASET_NAME = "xz2_intersect_query_test";
  static SpatialQueryCondition spatialIntersectQueryCondition;
  static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
      "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Envelope envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT).getEnvelopeInternal();
      Envelope envelopeContained = wktReader.read(QUERY_WKT_CONTAIN).getEnvelopeInternal();
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition = new SpatialQueryCondition(envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
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
        new XZ2IndexStrategy(),
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
  public void testExecuteIntersectQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialIntersectQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    for (Trajectory result : results) {
      ByteArray index = dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy()
          .index(result);
      System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy().parseIndex2String(index));
      ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
    }
    assert spatialQuery.executeQuery().size() == 13;
  }

  @Test
  public void testExecuteContainedQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialContainedQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    for (Trajectory result : results) {
      System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy().index(result));
    }
    assertEquals(spatialQuery.executeQuery().size(), 19);
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}