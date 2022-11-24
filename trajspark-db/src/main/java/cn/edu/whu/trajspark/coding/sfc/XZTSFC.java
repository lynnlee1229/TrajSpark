package cn.edu.whu.trajspark.coding.sfc;

import static cn.edu.whu.trajspark.constant.CodingConstants.LOG_FIVE;

import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeElement;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xu Qi
 * @since 2022/10/6
 */
public class XZTSFC implements Serializable {

  private final int g;
  private final TimePeriod timePeriod;
  private static final Logger LOGGER = LoggerFactory.getLogger(XZTSFC.class);

  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
  private final TimeElement initializeTime = new TimeElement(0, 1);
  TimeElement levelSeparator = new TimeElement(-1, -1);

  public XZTSFC(int g, TimePeriod timePeriod) {
    this.g = g;
    this.timePeriod = timePeriod;
  }

  public static XZTSFC apply(int g, TimePeriod timePeriod) {
    return new XZTSFC(g, timePeriod);
  }

  /**
   * Get time coding
   *
   * @param timeLine With time starting point and end point information
   * @param timeBin  Time interval information
   * @return Time coding
   */
  public long index(TimeLine timeLine, TimeBin timeBin) {
    double[] binTime = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin, true);
    double length = binTime[1] - binTime[0];
    int l1 = (int) Math.floor(Math.log(length) / LOG_FIVE);
    int level;
    if (l1 >= g) {
      level = g;
    } else {
      double length2 = Math.pow(0.5, l1 + 1);
      if (predicate(binTime[0], binTime[1], length2)) {
        level = l1 + 1;
      } else {
        level = l1;
      }
    }
    return sequenceCode(binTime[0], level);
  }

  public List<TimeIndexRange> ranges(TimeLine timeLine) {
    List<TimeLine> timeLineList = new ArrayList<>(1);
    timeLineList.add(timeLine);
    return ranges(timeLineList);
  }

  public List<TimeIndexRange> ranges(List<TimeLine> timeLineList) {
    List<TimeIndexRange> ranges = new ArrayList<>(500);
    Deque<TimeElement> remaining = new ArrayDeque<>(500);
    Boolean checkBin = checkBin(timeLineList);
    if (checkBin) {
      return innerBinRanges(timeLineList, ranges, remaining);
    } else {
      return outBinRanges(timeLineList, ranges, remaining);
    }
  }

  public Boolean checkBin(List<TimeLine> timeLineList) {
    TimeLine timeLine1 = timeLineList.get(0);
    List<TimeBin> timeBinList1 = getTimeBinList(timeLine1);
    //Time-only physical bounding box
    TimeBin timeBin = timeBinList1.get(1);
    for (TimeLine timeLine : timeLineList) {
      List<TimeBin> timeBinList = getTimeBinList(timeLine);
      for (int i = 1; i < timeBinList.size(); i++) {
        if (timeBinList.get(i).equals(timeBin)) {
          continue;
        } else {
          return false;
        }
      }
    }
    return true;
  }

  public List<TimeIndexRange> innerBinRanges(List<TimeLine> timeLineList,
      List<TimeIndexRange> ranges,
      Deque<TimeElement> remaining) {
    List<TimeBin> timeBinList = getTimeBinList(timeLineList.get(0));
    for (TimeBin timeBin : timeBinList) {
      List<TimeLine> timeLines = timeToRefBinLong(timeLineList, timeBin);
      short level = 0;
      remaining.add(initializeTime);
      remaining.add(levelSeparator);
      while (level < g && !remaining.isEmpty()) {
        TimeElement next = remaining.poll();
        if (next.equals(levelSeparator)) {
          // we've fully processed a level, increment our state
          if (!remaining.isEmpty()) {
            level = (short) (level + 1);
            remaining.add(levelSeparator);
          }
        } else {
          ranges = innerBinSequenceCodeRange(next, timeLines, level, ranges, remaining, timeBin);
        }
      }
      ranges = getMaxLevelSequenceCodeRange(level, ranges,
          remaining, timeBin);
    }
    ranges = getCodeRangeKeyMerge(ranges);
    return ranges;
  }

  public List<TimeIndexRange> outBinRanges(List<TimeLine> timeLineList,
      List<TimeIndexRange> ranges,
      Deque<TimeElement> remaining) {
    for (TimeLine timeLine : timeLineList) {
      List<TimeBin> timeBinList = getTimeBinList(timeLine);
      for (TimeBin timeBin : timeBinList) {
        double[] doubles = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin,
            false);
        TimeLine line = new TimeLine(doubles[0], doubles[1]);
        short level = 0;
        remaining.add(initializeTime);
        remaining.add(levelSeparator);
        while (level < g && !remaining.isEmpty()) {
          TimeElement next = remaining.poll();
          if (next.equals(levelSeparator)) {
            // we've fully processed a level, increment our state
            if (!remaining.isEmpty()) {
              level = (short) (level + 1);
              remaining.add(levelSeparator);
            }
          } else {
            ranges = getSequenceCodeRange(line, next, level, ranges, remaining, timeBin);
          }
        }
        ranges = getMaxLevelSequenceCodeRange(level, ranges,
            remaining, timeBin);
      }
    }
    ranges = getCodeRangeKeyMerge(ranges);
    return ranges;
  }

  public Boolean isContained(TimeElement timeElement, List<TimeLine> timeLineList) {
    int i = 0;
    while (i < timeLineList.size()) {
      if (timeElement.isContained(timeLineList.get(i))) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  public Boolean isOverlapped(TimeElement timeElement, List<TimeLine> timeLineList) {
    int i = 0;
    while (i < timeLineList.size()) {
      if (timeElement.isOverlaps(timeLineList.get(i))) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  public List<TimeIndexRange> innerBinSequenceCodeRange(TimeElement timeElement,
      List<TimeLine> timeLineList,
      short level, List<TimeIndexRange> ranges, Deque<TimeElement> remaining, TimeBin timeBin) {
    if (isContained(timeElement, timeLineList)) {
      TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          false);
      ranges.add(timeIndexRange);
    } else if (isOverlapped(timeElement, timeLineList)) {
      // some portion of this range is excluded
      // add the partial match and queue up each sub-range for processing
      TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          true);
      ranges.add(timeIndexRange);
      remaining.addAll(timeElement.getChildren());
    }
    return ranges;
  }

  public List<TimeLine> timeToRefBinLong(List<TimeLine> timeLineList, TimeBin timeBin) {
    List<TimeLine> timeLineArrayList = new ArrayList<>();
    for (TimeLine timeLine : timeLineList) {
      double[] doubles = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin,
          false);
      TimeLine line = new TimeLine(doubles[0], doubles[1]);
      timeLineArrayList.add(line);
    }
    return timeLineArrayList;
  }

  public List<TimeIndexRange> getSequenceCodeRange(TimeLine timeLine, TimeElement timeElement,
      short level, List<TimeIndexRange> ranges, Deque<TimeElement> remaining, TimeBin timeBin
  ) {
    if (timeElement.isContained(timeLine)) {
      // whole range matches
      TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          false);
      ranges.add(timeIndexRange);
    } else if (timeElement.isOverlaps(timeLine)) {
      // some portion of this range is excluded
      // add the partial match and queue up each sub-range for processing
      TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          true);
      ranges.add(timeIndexRange);
      remaining.addAll(timeElement.getChildren());
    }
    return ranges;
  }

  public List<TimeIndexRange> getMaxLevelSequenceCodeRange(
      short level, List<TimeIndexRange> ranges, Deque<TimeElement> remaining, TimeBin timeBin
  ) {
    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    while (!remaining.isEmpty()) {
      TimeElement poll = remaining.poll();
      if (poll.equals(levelSeparator)) {
        level = (short) (level + 1);
      } else {
        TimeIndexRange timeIndexRange = sequenceInterval(poll.getTimeStart(), level, timeBin,
            false);
        timeIndexRange.setContained(false);
        ranges.add(timeIndexRange);
      }
    }
    return ranges;
  }

  public List<TimeIndexRange> getCodeRangeKeyMerge(List<TimeIndexRange> ranges) {
    ranges.sort(
        Comparator.comparing(TimeIndexRange::getBin).thenComparing(TimeIndexRange::getLower));
    List<TimeIndexRange> result = new ArrayList<>();
    TimeIndexRange current = ranges.get(0);
    int i = 1;
    while (i < ranges.size()) {
      TimeIndexRange indexRange = ranges.get(i);
      if (indexRange.getTimeBin().equals(current.getTimeBin())
          & indexRange.getLower() == current.getUpper() + 1
          & indexRange.isContained() == current.isContained()) {
        // merge the two ranges
        current = new TimeIndexRange(current.getLower(),
            Math.max(current.getUpper(), indexRange.getUpper()), indexRange.getTimeBin(),
            indexRange.isContained());
      } else {
        // append the last range and set the current range for future merging
        result.add(current);
        current = indexRange;
      }
      i += 1;
    }
    result.add(current);
    return result;
  }

  public TimeIndexRange sequenceInterval(double timeStart, short level, TimeBin timeBin,
      Boolean flag) {
    long min = sequenceCode(timeStart, level);
    long max;
    if (flag) {
      max = min;
    } else {
      //Hbase RowKey Scan
      max = min + (long) (Math.pow(2, g - level + 1) - 1L) - 1;
    }
    return new TimeIndexRange(min, max, timeBin, !flag);
  }

  public List<TimeBin> getTimeBinList(TimeLine timeLine) {
    short binIdStart = (short) timePeriod.getChronoUnit().between(Epoch, timeLine.getTimeStart());
    short binIDEnd = (short) timePeriod.getChronoUnit().between(Epoch, timeLine.getTimeEnd());
    List<TimeBin> timeBins = new ArrayList<>();
    for (int i = binIdStart - 1; i < binIDEnd + 1; i++) {
      TimeBin bin = new TimeBin((short) i, timePeriod);
      timeBins.add(bin);
    }
    return timeBins;
  }

  public double[] normalize(ZonedDateTime startTime, ZonedDateTime endTime, TimeBin timeBin,
      Boolean flag) {
    double nStart = (timeBin.getRefTime(startTime) * 1.0) / timePeriod.getChronoUnit().getDuration()
        .getSeconds();
    double nEnd = 0.0;
    if (flag) {
      if (timePeriod.getChronoUnit().getDuration()
          .compareTo(Duration.ofSeconds(timeBin.getRefTime(endTime) / 2)) > 0) {
        nEnd =
            timeBin.getRefTime(endTime) * 1.0 / timePeriod.getChronoUnit().getDuration()
                .getSeconds();
      } else {
        LOGGER.error(
            "The timeBin granules are too small to accommodate this length of time,please adopt a larger time granularity");
        throw new IllegalArgumentException();
      }
    } else {
      nEnd =
          timeBin.getRefTime(endTime) * 1.0 / timePeriod.getChronoUnit().getDuration().getSeconds();
    }
    return new double[]{nStart, nEnd};
  }

  public Boolean predicate(double min, double max, double length) {
    return max <= (Math.floor(min / length) * length) + (2 * length);
  }

  public long sequenceCode(double timeStart, int level) {
    double timeMin = 0.0;
    double timeMax = 1.0;
    long indexCode = 0L;
    int i = 0;
    while (i < level) {
      double timeCenter = (timeMin + timeMax) / 2.0;
      if (timeStart - timeCenter < 0) {
        indexCode += 1L;
        timeMax = timeCenter;
      } else {
        indexCode += 1L + (long) (Math.pow(2, g - i) - 1L);
        timeMin = timeCenter;
      }
      i += 1;
    }
    return indexCode;
  }
}
