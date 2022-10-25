package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeIndexRange;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static cn.edu.whu.trajspark.constant.CodingConstants.DEFAULT_TIME_PERIOD;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TIME_BIN_PRECISION;

/**
 * @author Haocheng Wang Created on 2022/10/2
 */
public class TimeLineCoding implements TimeCoding {


  private final int g;
  private final TimePeriod timePeriod;
  private XZTCoding xztCoding;

  @SuppressWarnings("checkstyle:StaticVariableName")
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
  private static Logger logger = LoggerFactory.getLogger(TimeLineCoding.class);

  public TimeLineCoding() {
    g = MAX_TIME_BIN_PRECISION;
    timePeriod = DEFAULT_TIME_PERIOD;
    xztCoding = XZTCoding.apply(g, timePeriod);
  }

  public TimeLineCoding(int g, TimePeriod timePeriod) {
    if (g > MAX_TIME_BIN_PRECISION) {
      logger.error(
          "Only support time bin precision lower or equal than {}," + " but found precision is {}",
          MAX_TIME_BIN_PRECISION, g);
    }
    this.g = g;
    this.timePeriod = timePeriod;
  }

  @Override
  public List<TimeIndexRange> ranges(TemporalQueryCondition condition) {
    List<TimeIndexRange> indexRangeList = new ArrayList<>(500);
    if (condition.getQueryWindows() == null) {
      if (condition.getTemporalQueryType() == TemporalQueryType.OVERLAP) {
        indexRangeList = xztCoding.ranges(condition.getQueryWindow(), timePeriod);
      } else if (condition.getTemporalQueryType() == TemporalQueryType.INCLUDE) {
        indexRangeList = xztCoding.ranges(condition.getQueryWindow(), timePeriod);
      }
    } else {
      if (condition.getTemporalQueryType() == TemporalQueryType.OVERLAP) {
        indexRangeList = xztCoding.ranges(condition.getQueryWindows(), timePeriod);
      } else if (condition.getTemporalQueryType() == TemporalQueryType.INCLUDE) {
        indexRangeList = xztCoding.ranges(condition.getQueryWindows(), timePeriod);
      }
    }
    return indexRangeList;
  }

  public XZTCoding getXztCoding() {
    return xztCoding;
  }

  @Override
  public TimePeriod getTimePeriod() {
    return timePeriod;
  }


  public TimeBin getTrajectoryTimeBin(TrajFeatures features) {
    ZonedDateTime zonedDateTime = features.getStartTime();
    return dateToBinnedTime(zonedDateTime);
  }

  public TimeBin epochSecondToBinnedTime(long time) {
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
        ZoneOffset.UTC);
    return dateToBinnedTime(zonedDateTime);
  }

  public TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime) {
    short binId = (short) timePeriod.getChronoUnit().between(Epoch, zonedDateTime);
    return new TimeBin(binId, timePeriod);
  }

  public long getIndex(ZonedDateTime start, ZonedDateTime end) {
    return getIndex(new TimeLine(start, end));
  }

  /**
   * @param timeLine With time starting point and end point information
   * @return Time coding
   */
  @Override
  public long getIndex(TimeLine timeLine) {
    TimeBin bin = dateToBinnedTime(timeLine.getTimeStart());
    return xztCoding.index(timeLine, bin);
  }

  /**
   * @param coding Time coding
   * @return Sequences 0 and 1 of time
   */
  public List<Integer> getSequenceCode(long coding) {
    int g = this.g;
    List<Integer> list = new ArrayList<>(g);
    for (int i = 0; i < g; i++) {
      if (coding <= 0) {
        break;
      }
      long operator = (long) Math.pow(2, g - i) - 1;
      long s = ((coding - 1) / operator);
      list.add((int) s);
      coding = coding - 1L - s * operator;
    }
    return list;
  }

  /**
   * Obtaining Minimum Time Bounding Box Based on Coding Information
   *
   * @param coding  Time coding
   * @param timeBin Time interval information
   * @return With time starting point and end point information
   */
  public TimeLine getTimeLine(long coding, TimeBin timeBin) {
    List<Integer> list = getSequenceCode(coding);
    double timeMin = 0.0;
    double timeMax = 1.0;
    for (Integer integer : list) {
      double timeCenter = (timeMin + timeMax) / 2;
      if (integer == 0) {
        timeMax = timeCenter;
      } else {
        timeMin = timeCenter;
      }
    }
    ZonedDateTime binStartTime = timeBinToDate(timeBin);
    long timeStart = (long) (timeMin * timePeriod.getChronoUnit().getDuration().getSeconds())
        + binStartTime.toEpochSecond();
    long timeEnd = (long) (
        (2 * (timeMax * timePeriod.getChronoUnit().getDuration().getSeconds()) - (timeMin
            * timePeriod.getChronoUnit().getDuration().getSeconds()))
            + binStartTime.toEpochSecond());
    ZonedDateTime startTime = timeToZonedTime(timeStart);
    ZonedDateTime endTime = timeToZonedTime(timeEnd);

    return new TimeLine(startTime, endTime);
  }

  public static ZonedDateTime timeBinToDate(TimeBin binnedTime) {
    long bin = binnedTime.getBin();
    return binnedTime.getTimePeriod().getChronoUnit().addTo(Epoch, bin);
  }

  public int timeToBin(long time) {
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
        ZoneOffset.UTC);
    return dateToBin(zonedDateTime);
  }

  public int dateToBin(ZonedDateTime zonedDateTime) {
    long binId = timePeriod.getChronoUnit().between(Epoch, zonedDateTime);
    return (int) binId;
  }

  public ZonedDateTime timeToZonedTime(long time) {
    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
        ZoneOffset.UTC);
  }

}
