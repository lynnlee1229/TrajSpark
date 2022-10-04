package cn.edu.whu.trajspark.datatypes;

import java.time.temporal.ChronoUnit;

/**
 * @author Haocheng Wang
 * Created on 2022/10/2
 */
public enum TimePeriod {
  DAY(ChronoUnit.DAYS),
  WEEK(ChronoUnit.WEEKS),
  MONTH(ChronoUnit.MONTHS),
  YEAR(ChronoUnit.YEARS);

  ChronoUnit chronoUnit;

  TimePeriod(ChronoUnit chronoUnit) {
    this.chronoUnit = chronoUnit;
  }

  public ChronoUnit getChronoUnit() {
    return chronoUnit;
  }
}
