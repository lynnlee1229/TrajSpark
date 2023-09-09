package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.sfc.TimeIndexRange;
import cn.edu.whu.trajspark.coding.sfc.XZTSFC;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import junit.framework.TestCase;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.DEFAULT_TIME_PERIOD;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TIME_BIN_PRECISION;
import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;


/**
 * @author Xu Qi
 * @since 2022/10/8
 */
class XZTCodingTest extends TestCase {

  @Test
  public void getIndex() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(XZTCoding);
    ByteArray index = IDTIndexStrategy.index(exampleTrajectory);
    TimeLine timeLineRange = IDTIndexStrategy.getTimeLineRange(index);
    System.out.println("ByteArray: " + index);
    System.out.println(
        "timeStart: " + exampleTrajectory.getTrajectoryFeatures().getStartTime() + "timeEnd: "
            + exampleTrajectory.getTrajectoryFeatures().getEndTime());
    System.out.println(timeLineRange);
    TimeBin timeBinVal = IDTIndexStrategy.getTimeBin(index);
    long timeCodingVal = IDTIndexStrategy.getTimeElementCode(index);
    List<Integer> sequenceCode = XZTCoding.getSequenceCode(timeCodingVal);
    long coding = 0L;
    for (int i = 0; i < sequenceCode.size(); i++) {
      coding += 1L + sequenceCode.get(i) * ((long) Math.pow(2, MAX_TIME_BIN_PRECISION - i) - 1L);
    }
    System.out.println("timeBinVal: " + timeBinVal);
    System.out.println("timeCodingVal: " + timeCodingVal);
    System.out.println("coding: " + coding);
    assert timeCodingVal == coding;
  }


  @Test
  public void getCodingRanges() {
    ZonedDateTime base = ZonedDateTime.of(2015, 12,26,12,0,0,0, ZoneOffset.UTC);
    XZTSFC xztsfc = new XZTSFC(MAX_TIME_BIN_PRECISION, DEFAULT_TIME_PERIOD);
    List<TimeIndexRange> list = xztsfc.ranges(new TimeLine(base, base.plus(10, ChronoUnit.MINUTES)), false);
    for (TimeIndexRange range : list) {
      System.out.println();
      System.out.println();
      System.out.println(range.getLowerXZTCode());
      System.out.println(range.getUpperXZTCode());
      System.out.println(new XZTCoding().getXZTElementTimeLine(range.getLowerXZTCode()));
      System.out.println(new XZTCoding().getXZTElementTimeLine(range.getUpperXZTCode()));
      System.out.println(range.isContained());
      System.out.println("length: " + (range.getUpperXZTCode() - range.getLowerXZTCode()));
    }
  }

  @Test
  public void getXZTCode() {
    ZonedDateTime start = ZonedDateTime.of(2015,12,26,11,53,12,0, ZoneId.of("UTC"));
    ZonedDateTime end = ZonedDateTime.of(2015,12,26,12,24,13,0, ZoneId.of("UTC"));
    TimeBin timeBin = new TimeBin(16795, TimePeriod.DAY);
    System.out.println(new XZTCoding().getXZTElementTimeLine(new XZTSFC(MAX_TIME_BIN_PRECISION, TimePeriod.DAY).index(new TimeLine(start, end), timeBin)));
  }

  @Test
  public void getXZTCode2() {
    ZonedDateTime start = ZonedDateTime.of(2015,12,26,11,53,12,0, ZoneId.of("UTC"));
    ZonedDateTime end = ZonedDateTime.of(2015,12,26,12,24,13,0, ZoneId.of("UTC"));
    TimeBin timeBin = new TimeBin(16795, TimePeriod.DAY);
    System.out.println(new XZTCoding().getXZTElementTimeLine(new XZTSFC(MAX_TIME_BIN_PRECISION, TimePeriod.DAY).index(new TimeLine(start, end), timeBin)));
    // System.out.println(new XZTCoding().getXZTElementTimeLine(4282649));
  }
  }