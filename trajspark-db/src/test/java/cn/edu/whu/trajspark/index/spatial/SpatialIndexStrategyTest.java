package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.coding.XZ2PlusCoding;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class SpatialIndexStrategyTest extends TestCase {

  public static Trajectory getExampleTrajectory() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    ZonedDateTime start = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2022-01-01 12:00:00", dateTimeFormatter);
    TrajFeatures trajFeatures = new TrajFeatures(
        start,
        end,
        new TrajPoint(start, 114.364672,30.535034),
        new TrajPoint(end, 114.378505,30.544039),
        2,
        null,
        0,
        2);
    return new Trajectory(
        "Trajectory demo",
        "001",
        Arrays.asList(
            new TrajPoint(start, 114.364672,30.535034),
            new TrajPoint(end, 114.378505,30.544039)),
        trajFeatures);
  }

  public void testIndex() {
    Trajectory t = getExampleTrajectory();
    SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 0);
    System.out.println(spatialIndexStrategy.index(t));
  }

  public void testGetSpatialRange() {
    Trajectory t = getExampleTrajectory();
    SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 0);
    ByteArray byteArray = spatialIndexStrategy.index(t);
    WKTWriter wktWriter = new WKTWriter();
    System.out.println("xz2 grid: " + wktWriter.write(spatialIndexStrategy.getSpatialRange(byteArray)));
    System.out.println("trajectory envelope: " + wktWriter.write(t.getLineString().getEnvelope()));

  }

  public void testGetScanRanges() {
    WKTReader reader = new WKTReader();
    try {
      Geometry geom = reader.read("POLYGON ((114.345703125 30.531005859375, 114.345703125 30.5419921875, 114.36767578125 30.5419921875, 114.36767578125 30.531005859375, 114.345703125 30.531005859375))");
      SpatialQueryCondition spatialQueryCondition = new SpatialQueryCondition(geom.getEnvelopeInternal(), SpatialQueryCondition.SpatialQueryType.OVERLAP);
      SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 1);
      List<RowKeyRange> list = spatialIndexStrategy.getScanRanges(spatialQueryCondition);
      for (RowKeyRange range : list) {
        System.out.println("start:" + spatialIndexStrategy.indexToString(range.getStartKey()) + "end: " + spatialIndexStrategy.indexToString(range.getEndKey()));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testIndexToString() {
    Trajectory t = getExampleTrajectory();
    SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 0);
    ByteArray byteArray = spatialIndexStrategy.index(t);
    System.out.println(spatialIndexStrategy.indexToString(byteArray));
  }

  public void testGetShardNum() {
  }

  public void testGetTrajectoryId() {
    Trajectory t = getExampleTrajectory();
    SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 0);
    ByteArray byteArray = spatialIndexStrategy.index(t);
    assertEquals(spatialIndexStrategy.getTrajectoryId(byteArray), t.getTrajectoryID());
  }
}