package cn.edu.whu.trajspark.coding;

import static cn.edu.whu.trajspark.coding.conf.Constants.LOG_FIVE;

import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import java.time.Duration;
import java.time.ZonedDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Xu Qi
 * @since 2022/10/6
 */
public class XZTCoding {

  private final int g;
  private final TimePeriod timePeriod;
  private static final Logger LOGGER = LoggerFactory.getLogger(XZTCoding.class);

  public XZTCoding(int g, TimePeriod timePeriod) {
    this.g = g;
    this.timePeriod = timePeriod;
  }

  public static XZTCoding apply(int g, TimePeriod timePeriod) {
    return new XZTCoding(g, timePeriod);
  }

  /**
   * Get time coding
   *
   * @param timeLine With time starting point and end point information
   * @param timeBin  Time interval information
   * @return Time coding
   */
  public long index(TimeLine timeLine, TimeBin timeBin) {
    double[] binTime = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin);
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

  public double[] normalize(ZonedDateTime startTime, ZonedDateTime endTime, TimeBin timeBin) {
    double nStart = (timeBin.getRefTime(startTime) * 1.0) / timePeriod.getChronoUnit().getDuration()
        .getSeconds();
    double nEnd;
    if (timePeriod.getChronoUnit().getDuration()
        .compareTo(Duration.ofSeconds(timeBin.getRefTime(endTime) / 2)) > 0) {
      nEnd =
          timeBin.getRefTime(endTime) * 1.0 / timePeriod.getChronoUnit().getDuration().getSeconds();
    } else {
      LOGGER.error(
          "The timeBin granules are too small to accommodate this length of time,please adopt a larger time granularity");
      throw new IllegalArgumentException();
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
