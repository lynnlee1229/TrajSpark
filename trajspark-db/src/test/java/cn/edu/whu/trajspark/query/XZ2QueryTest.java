package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/27
 */
public class XZ2QueryTest extends TestCase {

  static String DATASET_NAME = "xz2_intersect_query_test";
  static SpatialQueryCondition spatialIntersectQueryCondition;
  static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
          "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";
  static Database instance;
  static IndexMeta indexMeta = new IndexMeta(true, new XZ2IndexStrategy(), DATASET_NAME, "default");
  static IndexTable indexTable;

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      instance = Database.getInstance();
      WKTReader wktReader = new WKTReader();
      Envelope envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT).getEnvelopeInternal();
      Envelope envelopeContained = wktReader.read(QUERY_WKT_CONTAIN).getEnvelopeInternal();
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition = new SpatialQueryCondition(envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException | IOException e) {
      e.printStackTrace();
    }
  }

@Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(indexMeta);
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
  }

 @Test
  public void testExecuteIntersectQuery() throws IOException {
    Database instance = Database.getInstance();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
   indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory result : results) {
      ByteArray index = indexTable.getIndexMeta().getIndexStrategy()
          .index(result);
      System.out.println(indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
    }
   assertEquals(spatialQuery.executeQuery().size(), 13);
  }

  @Test
  public void testExecuteContainedQuery() throws IOException {
    Database instance = Database.getInstance();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialContainedQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory result : results) {
      System.out.println(indexTable.getIndexMeta().getIndexStrategy().index(result));
    }
    assertEquals(spatialQuery.executeQuery().size(), 19);
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }

  @Test
  public void testGetAnswer() throws URISyntaxException, ParseException, IOException {
    Database instance = Database.getInstance();
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    WKTReader wktReader = new WKTReader();
    Polygon envelope = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
    Polygon envelope1 = (Polygon) wktReader.read(QUERY_WKT_INTERSECT).getEnvelope();
    System.out.println(trips.size());
    int i = 0;
    int j = 0;
    indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory trajectory : trips) {
      if (envelope.contains(trajectory.getLineString())) {
        ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(trajectory);
        System.out.println(indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
        i++;
      }
    }
    System.out.println("CONTAIN: " + i);
    for (Trajectory trajectory : trips) {
      if (envelope1.intersects(trajectory.getLineString())) {
        ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(trajectory);
        System.out.println(indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
        j++;
      }
    }
    System.out.println("INTERSECT: " + j);
  }

//  public void testAllScanTrajectories() {
//    Database instance = Database.getInstance();
//    try {
//      instance.openConnection();
//      DataTable dataTable = instance.getDataTable(DATASET_NAME);
//      SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialIntersectQueryCondition);
//      List<RowKeyRange> list = spatialQuery.getIndexRanges();
//      List<Range> rangeList = new LinkedList<>();
//      for (RowKeyRange range : list) {
//        rangeList.add(Range.newBuilder()
//            .setStart(ByteString.copyFrom(range.getStartKey().getBytes()))
//            .setEnd(ByteString.copyFrom(range.getEndKey().getBytes()))
//            .setContained(range.isContained()).build());
//      }
//      for (Range range : rangeList) {
//        RowKeyRange r = new RowKeyRange(new ByteArray(range.getStart().toByteArray()), new ByteArray(range.getEnd().toByteArray()));
//        List<Result> resultList = dataTable.scan(r);
//        if (resultList.size() > 0) {
//          System.out.println("Scan range: " + r);
//        }
//        for (Result result : resultList) {
//          Envelope envelope = spatialIntersectQueryCondition.getQueryWindow();;
//          Polygon queryPolygon = new MinimumBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()).toPolygon(4326);
//          Trajectory t = TrajectorySerdeUtils.getTrajectory(result);
//          if (queryPolygon.intersects(t.getLineString())) {
//            System.out.println(new ByteArray(result.getRow()));
//          }
//        }
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
@Test
  public void testSingleGetTrajectory() throws IOException {
    byte[] target = Bytes.fromHex("000000000000000000012156ad414342514244533537");
    Result result = indexTable.get(new Get(target));
    Envelope envelope = spatialContainedQueryCondition.getQueryWindow();
    Polygon queryPolygon = new MinimumBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()).toPolygon(4326);
    Trajectory t = TrajectorySerdeUtils.mainRowToTrajectory(result);
    System.out.println(queryPolygon.contains(t.getLineString()));
  }
}