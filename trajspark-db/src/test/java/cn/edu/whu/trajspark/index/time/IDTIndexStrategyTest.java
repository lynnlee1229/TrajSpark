package cn.edu.whu.trajspark.index.time;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.TIME_ZONE;
import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Xu Qi
 * @since 2022/10/16
 */
class IDTIndexStrategyTest extends TestCase {

  @Test
  public void testIndex() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(XZTCoding);
    ByteArray index = IDTIndexStrategy.index(exampleTrajectory);
    System.out.println("ByteArray: " + index);
  }

  @Test
  public void testGetTimeRange() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(XZTCoding);
    ByteArray index = IDTIndexStrategy.index(exampleTrajectory);
    TimeLine timeLineRange = IDTIndexStrategy.getTimeLineRange(index);
    System.out.println(
        "timeStart: " + exampleTrajectory.getTrajectoryFeatures().getStartTime() + "timeEnd: "
            + exampleTrajectory.getTrajectoryFeatures().getEndTime());
    System.out.println(timeLineRange);
  }


  @Test
  void getSingleScanRanges() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(TIME_ZONE);
    ZonedDateTime start = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2022-01-02 12:00:00", dateTimeFormatter);
    TimeLine timeLine = new TimeLine(start, end);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLine,
        TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Single ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey()) + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey()) + " isContained "
              + scanRange.isContained());
    }
  }

  @Test
  public void testGetMultiScanRange() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(TIME_ZONE);
    ZonedDateTime start1 = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2022-01-02 12:00:00", dateTimeFormatter);
    TimeLine timeLine1 = new TimeLine(start1, end1);
    ZonedDateTime start2 = ZonedDateTime.parse("2022-01-02 14:00:00", dateTimeFormatter);
    ZonedDateTime end2 = ZonedDateTime.parse("2022-01-03 16:00:00", dateTimeFormatter);
    TimeLine timeLine2 = new TimeLine(start2, end2);
    ArrayList<TimeLine> timeLines = new ArrayList<>();
    timeLines.add(timeLine1);
    timeLines.add(timeLine2);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLines,
        TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Multi ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey()) + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey()));
    }
  }

  @Test
  public void testMultiInnerBinScan() {
    long start = System.currentTimeMillis();
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(TIME_ZONE);
    ZonedDateTime start1 = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2022-01-01 12:00:00", dateTimeFormatter);
    TimeLine timeLine1 = new TimeLine(start1, end1);
    ZonedDateTime start2 = ZonedDateTime.parse("2022-01-01 14:00:00", dateTimeFormatter);
    ZonedDateTime end2 = ZonedDateTime.parse("2022-01-01 16:00:00", dateTimeFormatter);
    TimeLine timeLine2 = new TimeLine(start2, end2);
    ArrayList<TimeLine> timeLines = new ArrayList<>();
    timeLines.add(timeLine1);
    timeLines.add(timeLine2);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLines,
        TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Multi InnerBin ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey()) + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey()));
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}