package cn.edu.whu.trajspark.coding.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

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
        ZoneId.systemDefault());
  }
}
