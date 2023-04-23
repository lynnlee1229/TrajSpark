package cn.edu.whu.trajspark.datatypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

import static cn.edu.whu.trajspark.constant.DBConstants.TIME_ZONE;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class TimeBin {

  /**
   * 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year
   */
  private final short bin;
  private final TimePeriod timePeriod;
  @SuppressWarnings("checkstyle:StaticVariableName")
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, TIME_ZONE);

  public TimeBin(short bin, TimePeriod timePeriod) {
    this.bin = bin;
    this.timePeriod = timePeriod;
  }

  public short getBin() {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeBin timeBin = (TimeBin) o;
    return bin == timeBin.bin && timePeriod == timeBin.timePeriod;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bin, timePeriod);
  }

  @Override
  public String toString() {
    return "TimeBin{" + "bin=" + bin + ", timePeriod=" + timePeriod + '}';
  }
}
