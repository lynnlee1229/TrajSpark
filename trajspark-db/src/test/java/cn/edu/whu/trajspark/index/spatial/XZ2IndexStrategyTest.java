package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class XZ2IndexStrategyTest extends TestCase {

  public static Trajectory getExampleTrajectory() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    ZonedDateTime start = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2022-01-01 10:20:00", dateTimeFormatter);
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
            new TrajPoint(start, 114.06896,22.542664),
            new TrajPoint(start.plus(5L, ChronoUnit.MINUTES), 114.08942,22.543316),
            new TrajPoint(start.plus(10L, ChronoUnit.MINUTES), 114.116684,22.547997),
            new TrajPoint(start.plus(15L, ChronoUnit.MINUTES), 114.118904,22.562414),
            new TrajPoint(start.plus(20L, ChronoUnit.MINUTES), 114.10953,22.59049)),
        trajFeatures);
  }

  public void testIndex() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    assert XZ2IndexStrategy.index(t).toString().equals("000200000000000000012156aab33030315472616a6563746f72792064656d6f");
  }

  public void testGetScanRanges() {
    WKTReader reader = new WKTReader();
    try {
      Geometry geom = reader.read("POLYGON ((114.345703125 30.531005859375, 114.345703125 30.5419921875, 114.36767578125 30.5419921875, 114.36767578125 30.531005859375, 114.345703125 30.531005859375))");
      SpatialQueryCondition spatialQueryCondition = new SpatialQueryCondition(geom.getEnvelopeInternal(), SpatialQueryCondition.SpatialQueryType.INTERSECT);
      XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
      List<RowKeyRange> list = XZ2IndexStrategy.getScanRanges(spatialQueryCondition);
      for (RowKeyRange range : list) {
        System.out.println("start:" + XZ2IndexStrategy.parseIndex2String(range.getStartKey()) + "end: " + XZ2IndexStrategy.parseIndex2String(range.getEndKey()));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testIndexToString() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    System.out.println(XZ2IndexStrategy.parseIndex2String(byteArray));
  }

  public void testGetShardNum() {
  }

  public void testGetTrajectoryId() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    assertEquals(XZ2IndexStrategy.getObjectTrajId(byteArray), t.getObjectID() + t.getTrajectoryID());
  }
}