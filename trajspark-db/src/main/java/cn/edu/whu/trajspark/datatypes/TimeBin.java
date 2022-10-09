package cn.edu.whu.trajspark.datatypes;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 *
 * @author Haocheng Wang Created on 2022/9/28
 */
public class TimeBin {

  /**
   * 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year
   */
  private final long bin;
  private final TimePeriod timePeriod;
  @SuppressWarnings("checkstyle:StaticVariableName")
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

  public TimeBin(long bin, TimePeriod timePeriod) {
    this.bin = bin;
    this.timePeriod = timePeriod;
  }

  public long getBin() {
    return bin;
  }

  public TimePeriod getTimePeriod() {
    return timePeriod;
  }

  /**
   * @return min date time of the bin (inclusive)
   */
  public ZonedDateTime getBinStartTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bin);
  }

  /**
   * @return max date time of the bin(exculsive)
   */
  public ZonedDateTime getBinEndTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, bin + 1);
  }

  public long getRefTime(ZonedDateTime refTime) {
    return refTime.toEpochSecond() - getBinStartTime().toEpochSecond();
  }
}
