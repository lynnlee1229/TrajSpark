package cn.edu.whu.trajspark.base.util;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang.StringUtils;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
public class BasicDateUtils {
  public BasicDateUtils() {
  }

  static String deaultFormat = "yyyy-MM-dd HH:mm:ss";
  static String defaultZoneId = "UTC+8";

  static DateTimeFormatter defaultFormatter =
      DateTimeFormatter.ofPattern(deaultFormat).withZone(ZoneId.of(defaultZoneId));

  public static ZonedDateTime timeToUTC(ZonedDateTime time) {
    return time.withZoneSameInstant(ZoneOffset.UTC);
  }

  public static ZonedDateTime parse(String time, DateTimeFormatter dateTimeFormatter) {
    return StringUtils.isEmpty(time.trim()) ? null : ZonedDateTime.parse(time.trim(), dateTimeFormatter);
  }

  public static String format(ZonedDateTime time, String pattern) {
    return time == null ? "" : DateTimeFormatter.ofPattern(pattern).format(time);
  }

  public static ZonedDateTime parseDate(String timeFormat) {
    return ZonedDateTime.parse(timeFormat, defaultFormatter);
  }

  public static ZonedDateTime parseDate(String timeFormat, String format, String zoneId) {
    if (StringUtils.isEmpty(format)) {
      format = "yyyy-MM-dd HH:mm:ss";
    }

    if (StringUtils.isEmpty(zoneId)) {
      zoneId = "UTC+8";
    }

    DateTimeFormatter timeFormatter =
        DateTimeFormatter.ofPattern(format).withZone(ZoneId.of(zoneId));
    return ZonedDateTime.parse(timeFormat, timeFormatter);
  }

}