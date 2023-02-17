package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.TXZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.XZ2TIndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import cn.edu.whu.trajspark.query.basic.SpatialTemporalQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.query.coprocessor.STQueryEndPoint;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.addCoprocessor;
import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.deleteCoprocessor;

/**
 * @author Xu Qi
 * @since 2022/12/4
 */
class SpatialTemporalQueryTest extends TestCase {


  // index strategies
  static IDTIndexStrategy IDTIndexStrategy =  IDTemporalQueryTest.IDTIndexStrategy;
  static XZ2IndexStrategy xz2IndexStrategy = new XZ2IndexStrategy();
  static XZ2TIndexStrategy xz2TIndexStrategy = new XZ2TIndexStrategy();


  static String DATASET_NAME = "Spatial_Temporal_query_test";
  static SpatialTemporalQueryCondition stQueryConditionContain;
  static SpatialTemporalQueryCondition stQueryConditionIntersect;
  static TXZ2IndexStrategy txz2IndexStrategy = new TXZ2IndexStrategy();
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
      "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";
  static List<TimeLine> timeLineList = new ArrayList<>();

  static {
    // 查询条件
    TemporalQueryCondition temporalContainQuery = IDTemporalQueryTest.temporalContainCondition;
    TemporalQueryCondition temporalIntersectQuery = IDTemporalQueryTest.temporalIntersectCondition;
    SpatialQueryCondition spatialIntersectQueryCondition = SpatialQueryTest.spatialIntersectQueryCondition;
    SpatialQueryCondition spatialContainedQueryCondition = SpatialQueryTest.spatialContainedQueryCondition;
    stQueryConditionContain = new SpatialTemporalQueryCondition(
        spatialIntersectQueryCondition, temporalIntersectQuery);
    stQueryConditionIntersect = new SpatialTemporalQueryCondition(
        spatialContainedQueryCondition, temporalContainQuery);
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    IndexMeta indexMeta1 = new IndexMeta(true, xz2TIndexStrategy, DATASET_NAME, "default");
    IndexMeta indexMeta0 = new IndexMeta(false, txz2IndexStrategy, DATASET_NAME, indexMeta1, "default");
    IndexMeta indexMeta2 = new IndexMeta(false, xz2IndexStrategy, DATASET_NAME, indexMeta1, "default");
    IndexMeta indexMeta3 = new IndexMeta(false, IDTIndexStrategy, DATASET_NAME, indexMeta1, "default");
    list.add(indexMeta0);
    list.add(indexMeta1);
    list.add(indexMeta2);
    list.add(indexMeta3);
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
    System.out.println("to put " + trips.size() + " trajectories");
  }

  @Test
  public void testAddCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
    String className = STQueryEndPoint.class.getCanonicalName();
    String jarPath = "hdfs://localhost:9000/coprocessor/trajspark-db-1.0-SNAPSHOT.jar";
    addCoprocessor(conf, tableName, className, jarPath);
  }

  @Test
  void TestDeleteCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DBConstants.DATA_TABLE_SUFFIX;
    deleteCoprocessor(conf, tableName);
  }

  @Test
  void getIndexRanges() throws IOException {
    Database instance = Database.getInstance();
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
        stQueryConditionContain);
    List<RowKeyRange> scanRanges = spatialTemporalQuery.getIndexRanges();
    System.out.println("ST_Query Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + xz2TIndexStrategy.parseIndex2String(scanRange.getStartKey()) + " end : "
              + xz2TIndexStrategy.parseIndex2String(scanRange.getEndKey()) + "isContained "
              + scanRange.isContained());
    }
  }

  @Test
  void executeINTERSECQuery() throws IOException, ParseException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
        stQueryConditionContain);
    List<Trajectory> trajectories = spatialTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    WKTReader wktReader = new WKTReader();
    for (Trajectory trajectory : trajectories) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
      System.out.println(indexTable.getIndexMeta().getIndexStrategy()
          .index(trajectory));
      Polygon envelopeINTERSECT = (Polygon) wktReader.read(QUERY_WKT_INTERSECT).getEnvelope();
      System.out.println("envelopeINTERSECT :  " + envelopeINTERSECT.intersects(trajectory.getLineString()));
    }
    assertEquals(spatialTemporalQuery.executeQuery().size(),5);
  }

  @Test
  void executeContainQuery() throws IOException, ParseException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(DATASET_NAME),
        stQueryConditionIntersect);
    List<Trajectory> trajectories = spatialTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
      System.out.println(indexTable.getIndexMeta().getIndexStrategy()
          .index(trajectory));
      WKTReader wktReader = new WKTReader();
      Polygon envelopeCONTAIN = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
      System.out.println("envelopeCONTAIN :  " + envelopeCONTAIN.contains(trajectory.getLineString()));
    }
    assertEquals(spatialTemporalQuery.executeQuery().size(),3);
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }

  @Test
  public void testGetAnswer() throws URISyntaxException, IOException, ParseException {
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    int i = 0;
    int j = 0;
    WKTReader wktReader = new WKTReader();
    Polygon envelope = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
    Polygon envelope1 = (Polygon) wktReader.read(QUERY_WKT_INTERSECT).getEnvelope();
    for (Trajectory trajectory : trips) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      if (envelope.contains(trajectory.getLineString())) {
        for (TimeLine timeLine : timeLineList) {
          if (timeLine.getTimeStart().toEpochSecond() <= startTime.toEpochSecond()
              && endTime.toEpochSecond() <= timeLine.getTimeEnd().toEpochSecond()
          ) {
            System.out.println(new TimeLine(startTime, endTime));
            i++;
          }
        }
      }
    }
    System.out.println("CONTAIN: " + i);
    for (Trajectory trajectory : trips) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      if (envelope1.intersects(trajectory.getLineString())) {
        for (TimeLine timeLine : timeLineList) {
          if (startTime.toEpochSecond() <= timeLine.getTimeEnd().toEpochSecond()
              && timeLine.getTimeStart().toEpochSecond() <= endTime.toEpochSecond()
          ) {
            System.out.println(new TimeLine(startTime, endTime));
            j++;
          }
        }
      }
    }
    System.out.println("INTERSECT: " + j);
  }
}