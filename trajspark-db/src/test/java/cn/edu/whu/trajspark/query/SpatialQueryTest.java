package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import junit.framework.TestCase;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Xu Qi
 * @since 2022/12/26
 */
public class SpatialQueryTest extends TestCase {

  static String DATASET_NAME = "xz2_intersect_query_test";
  public static SpatialQueryCondition spatialIntersectQueryCondition;
  public static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
          "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
          "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      Geometry envelopeContained = wktReader.read(QUERY_WKT_CONTAIN);
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition = new SpatialQueryCondition(envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(true, new XZ2IndexStrategy(), DATASET_NAME, "default"));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory t : trips) {
      t.getTrajectoryFeatures();
      indexTable.putForMainTable(t);
    }
  }

  @Test
  public void testExecuteIntersectQuery() throws IOException, URISyntaxException, ParseException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    for (Trajectory result : results) {
      ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
      System.out.println(indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
      ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
    }
    assertEquals(getIntersectQueryAnswer(QUERY_WKT_INTERSECT, false), spatialQuery.executeQuery().size());
  }

  private int getIntersectQueryAnswer(String queryWKT, boolean contain) throws URISyntaxException, ParseException {
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    WKTReader wktReader = new WKTReader();
    Geometry query = wktReader.read(queryWKT);
    int res = 0;
    for (Trajectory trajectory : trips) {
      if (contain && query.contains(trajectory.getLineString())) {
          res++;
      } else if (!contain && query.intersects(trajectory.getLineString())) {
        res++;
      }
    }
    return res;
  }

  @Test
  public void testExecuteContainedQuery() throws IOException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialContainedQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    for (Trajectory result : results) {
      ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
      System.out.println(indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
      ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
    }
    assertEquals(19, spatialQuery.executeQuery().size());
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}