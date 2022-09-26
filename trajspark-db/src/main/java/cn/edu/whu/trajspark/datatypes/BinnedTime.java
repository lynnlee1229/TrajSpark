package cn.edu.whu.trajspark.datatypes;

import java.time.ZonedDateTime;

/**
 * TODO: impl
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class BinnedTime {
  /**
   * 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year
   */
  int bin;
  /**
   * 在bin内的偏移量, 单位为milliseconds, seconds, or hours
   */
  long offset;

  BinnedTimeUtil.TimePeriod timePeriod;

  public BinnedTime(int bin, long offset, BinnedTimeUtil.TimePeriod timePeriod) {
    this.bin = bin;
    this.offset = offset;
    this.timePeriod = timePeriod;
  }

  /**
   *
   * @return min date time of the bin (inclusive)
   */
  ZonedDateTime getBinStartTime() {
    return null;
  }

  /**
   *
   * @return max date time of the bin(exculsive)
   */
  ZonedDateTime getBinEndTime() {
    return null;
  }
}
