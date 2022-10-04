package cn.edu.whu.trajspark.datatypes;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * TODO: impl
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class TimeBin {
  /**
   * 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year
   */
  long bin;
  TimePeriod timePeriod;
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

  public TimeBin(long bin, TimePeriod timePeriod) {
    this.bin = bin;
    this.timePeriod = timePeriod;
  }

  /**
   *
   * @return min date time of the bin (inclusive)
   */
  ZonedDateTime getBinStartTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bin);
  }

  /**
   *
   * @return max date time of the bin(exculsive)
   */
  ZonedDateTime getBinEndTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bin + 1);
  }
}
