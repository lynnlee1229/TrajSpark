package cn.edu.whu.trajspark.datatypes;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * TODO: impl
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class BinnedTimeUtil {

  public enum TimePeriod {
    DAY(ChronoUnit.DAYS),
    WEEK(ChronoUnit.WEEKS),
    MONTH(ChronoUnit.MONTHS),
    YEAR(ChronoUnit.YEARS);
    ChronoUnit chronoUnit;

    TimePeriod(ChronoUnit chronoUnit) {
      this.chronoUnit = chronoUnit;
    }
  }

  TimePeriod timeBinUnit;
  ChronoUnit offsetUnit;

  public BinnedTimeUtil(TimePeriod timeBinUnit) {
    this.timeBinUnit = timeBinUnit;
    this.offsetUnit = getCoupledOffsetUnit(timeBinUnit);
  }

  static ChronoUnit getCoupledOffsetUnit(TimePeriod timeBinUnit) {
    ChronoUnit offsetUnit;
    switch (timeBinUnit) {
      case DAY:
        offsetUnit = ChronoUnit.MILLIS;
        break;
      case WEEK:
      case MONTH:
        offsetUnit = ChronoUnit.SECONDS;
        break;
      default:
        offsetUnit = ChronoUnit.MINUTES;
    }
    return offsetUnit;
  }

  static ZonedDateTime BinnedTimeToDate(BinnedTime binnedTime) {
    return null;
  }

  BinnedTime TimeToBinnedTime(long time) {
    return null;
  }

  BinnedTime DateToBinnedTime(ZonedDateTime zonedDateTime) {
    return null;
  }

  int TimeToBin(long time) {
    return 0;
  }

  int DateToBin(ZonedDateTime zonedDateTime) {
    return 0;
  }
}
