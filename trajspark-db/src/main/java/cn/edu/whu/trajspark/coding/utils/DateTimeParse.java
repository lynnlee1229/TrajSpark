package cn.edu.whu.trajspark.coding.utils;

import java.time.Instant;
import java.time.ZonedDateTime;

import static cn.edu.whu.trajspark.constant.DBConstants.TIME_ZONE;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
public class DateTimeParse {

  /**
   * parse timestamp to ZonedDateTime
   * @param time timestamp
   * @return ZonedDateTime zonedDateTime
   */
  public static ZonedDateTime timeToZonedTime(long time) {
    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
        TIME_ZONE);
  }
}
