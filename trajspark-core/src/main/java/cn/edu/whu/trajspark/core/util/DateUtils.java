package cn.edu.whu.trajspark.core.util;

import cn.edu.whu.trajspark.core.common.constant.DateDefaultConstant;
import cn.edu.whu.trajspark.core.common.field.Field;
import cn.edu.whu.trajspark.core.enums.BasicDataTypeEnum;
import org.apache.commons.lang.StringUtils;

import javax.ws.rs.NotSupportedException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
public class DateUtils {
  public DateUtils() {
  }

  public static ZonedDateTime timeToUTC(ZonedDateTime time) {
    return time.withZoneSameInstant(ZoneOffset.UTC);
  }

  public static ZonedDateTime parse(String time, DateTimeFormatter dateTimeFormatter) {
    return StringUtils.isEmpty(time.trim()) ? null :
        ZonedDateTime.parse(time.trim(), dateTimeFormatter);
  }

  public static String format(ZonedDateTime time, String pattern) {
    return time == null ? "" : DateTimeFormatter.ofPattern(pattern).format(time);
  }

  public static ZonedDateTime parse(BasicDataTypeEnum type, String timeStr, Field timeField) {
    ZonedDateTime time;
    switch (type) {
      case DATE:
        time = parseDate(timeStr, timeField);
        break;
      case TIMESTAMP:
        time = parseTimeStamp(Long.parseLong(timeStr), timeField);
        break;
      default:
        throw new NotSupportedException("can't support time dataType like " + type);
    }

    return time;
  }

  private static ZonedDateTime parseTimeStamp(long timeStamp, Field timeField) {
    String MS = "ms";
    Instant instant;
    if ("ms".equals(timeField.getUnit().toLowerCase())) {
      instant = Instant.ofEpochMilli(timeStamp);
    } else {
      instant = Instant.ofEpochSecond(timeStamp);
    }

    ZoneId zoneId;
    if (StringUtils.isEmpty(timeField.getZoneId())) {
      zoneId = DateDefaultConstant.DEFAULT_ZONE_ID;
    } else {
      zoneId = ZoneId.of(timeField.getZoneId());
    }

    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  private static ZonedDateTime parseDate(String timeFormat, Field timeField) {
    String format = timeField.getFormat();
    String zoneId = timeField.getZoneId();
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

  private static ZonedDateTime parseDate(String timeFormat) {
    return parseDate(timeFormat, null, null);
  }

  private static ZonedDateTime parseDate(String timeFormat, String format, String zoneId) {
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
