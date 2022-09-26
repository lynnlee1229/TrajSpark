package cn.edu.whu.trajspark.core.common.constant;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
public class DateDefaultConstant {
  public static final String DEFAULT_ZONE_ID_STR = "UTC+8";
  public static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("UTC+8");
  public static final String DEFAULT_DATETIME_PATTERN_STR = "yyyy-MM-dd HH:mm:ss";
  public static final DateTimeFormatter DEFAULT_TIME_FORMATTER;

  public DateDefaultConstant() {
  }

  static {
    DEFAULT_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(DEFAULT_ZONE_ID);
  }
}
