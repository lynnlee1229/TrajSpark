package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import junit.framework.TestCase;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class SpatialIndexStrategyTest extends TestCase {

  private Trajectory getExampleTrajectory() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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
        Arrays.<TrajPoint>asList(
            new TrajPoint(start, 114.364672,30.535034),
            new TrajPoint(end, 114.378505,30.544039)),
        trajFeatures);
  }

  public void testIndex() {
  }

  public void testGetSpatialRange() {
  }

  public void testGetScanRanges() {
  }

  public void testTestGetScanRanges() {
  }

  public void testTestGetScanRanges1() {
  }

  public void testTestGetScanRanges2() {
  }

  public void testIndexToString() {
  }

  public void testGetSpatialCoding() {
  }

  public void testGetSpatialCodingVal() {
  }

  public void testGetTimeCoding() {
  }

  public void testGetTimeCodingVal() {
  }

  public void testGetShardNum() {
  }
}