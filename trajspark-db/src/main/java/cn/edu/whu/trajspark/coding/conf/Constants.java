package cn.edu.whu.trajspark.coding.conf;

import cn.edu.whu.trajspark.datatypes.TimePeriod;

/**
 * @author Haocheng Wang
 * Created on 2022/9/27
 */
public class Constants {
  /**
   * Max length of xz2 Quadrant sequence
   */
  public static final short MAX_XZ2_PRECISION = 16;

  public static final double XZ2_X_MIN = -180.0;
  public static final double XZ2_X_MAX = 180.0;
  public static final double XZ2_Y_MIN = -90.0;
  public static final double XZ2_Y_MAX = 90.0;

  public static final short MAX_TIME_BIN_PRECISION = 8;

  public static final TimePeriod DEFAULT_TIME_PERIOD = TimePeriod.DAY;
}
