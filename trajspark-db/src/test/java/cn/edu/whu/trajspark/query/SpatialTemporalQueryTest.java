package cn.edu.whu.trajspark.query;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_SUFFIX;
import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.addCoprocessor;
import static cn.edu.whu.trajspark.query.coprocessor.CoprocessorLoader.deleteCoprocessor;
import static org.junit.jupiter.api.Assertions.*;

import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.TXZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatialtemporal.XZ2TIndexStrategy;
import cn.edu.whu.trajspark.index.time.TimeIndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition.SpatialQueryType;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.query.coprocessor.STQueryEndPoint;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Xu Qi
 * @since 2022/12/4
 */
class SpatialTemporalQueryTest extends TestCase {

  static String DATASET_NAME = "Spatial_Temporal_query_test";
  static SpatialTemporalQueryCondition spatialTemporalQueryCondition;
  static String Oid = "CBQBDS";
  static TXZ2IndexStrategy txz2IndexStrategy = new TXZ2IndexStrategy();
  static XZ2TIndexStrategy xz2TIndexStrategy = new XZ2TIndexStrategy();
  static XZ2IndexStrategy xz2IndexStrategy = new XZ2IndexStrategy();
  static TimeIndexStrategy timeIndexStrategy = new TimeIndexStrategy();
  static String QUERY_WKT = "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";

  static {
    try {
      WKTReader wktReader = new WKTReader();
      Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
      SpatialQueryCondition spatialQueryCondition = new SpatialQueryCondition(envelope,
          SpatialQueryType.INTERSECT);

      DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
          .withZone(ZoneId.systemDefault());
      ZonedDateTime start = ZonedDateTime.parse("2015-12-25 06:00:00", dateTimeFormatter);
      ZonedDateTime end = ZonedDateTime.parse("2015-12-25 07:00:00", dateTimeFormatter);
      ZonedDateTime start1 = ZonedDateTime.parse("2015-12-25 15:00:00", dateTimeFormatter);
      ZonedDateTime end1 = ZonedDateTime.parse("2015-12-25 16:00:00", dateTimeFormatter);
      TimeLine timeLine = new TimeLine(start, end);
      TimeLine timeLine1 = new TimeLine(start1, end1);
      List<TimeLine> timeLineList = new ArrayList<>();
      timeLineList.add(timeLine);
      timeLineList.add(timeLine1);
      TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLineList,
          TemporalQueryType.INTERSECT);
      spatialTemporalQueryCondition = new SpatialTemporalQueryCondition(
          spatialQueryCondition, temporalQueryCondition);
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
    IndexMeta indexMeta0 = new IndexMeta(false, txz2IndexStrategy, DATASET_NAME);
    IndexMeta indexMeta1 = new IndexMeta(true, xz2TIndexStrategy, DATASET_NAME);
    IndexMeta indexMeta2 = new IndexMeta(false, xz2IndexStrategy, DATASET_NAME);
    IndexMeta indexMeta3 = new IndexMeta(false, timeIndexStrategy, DATASET_NAME);
    list.add(indexMeta0);
    list.add(indexMeta1);
    list.add(indexMeta2);
    list.add(indexMeta3);
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
  }
  @Test
  public void testAddCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DATA_TABLE_SUFFIX;
    String className = STQueryEndPoint.class.getCanonicalName();
    String jarPath = "hdfs://localhost:9000/coprocessor/trajspark-db-1.0-SNAPSHOT.jar";
    addCoprocessor(conf, tableName, className, jarPath);
  }

  @Test
  void TestDeleteCoprocessor() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String tableName = DATASET_NAME + DATA_TABLE_SUFFIX;
    deleteCoprocessor(conf, tableName);
  }

  @Test
  void getIndexRanges() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(dataTable,
        spatialTemporalQueryCondition, IndexType.XZ2T);
    List<RowKeyRange> scanRanges = spatialTemporalQuery.getIndexRanges();
    System.out.println("ST_Query Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + txz2IndexStrategy.parseIndex2String(scanRange.getStartKey()) + " end : "
              + txz2IndexStrategy.parseIndex2String(scanRange.getEndKey()) + "isContained "
              + scanRange.isContained());
    }
  }

  @Test
  void executeQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(dataTable,
        spatialTemporalQueryCondition, IndexType.XZ2T);
    List<Trajectory> trajectories = spatialTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
      System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy()
          .index(trajectory));
    }
    assert spatialTemporalQuery.executeQuery().size() == 5;
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}