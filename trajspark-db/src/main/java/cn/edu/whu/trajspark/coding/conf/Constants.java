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
  public static final short MAX_XZ2_PRECISION = 32;

  public static final short MAX_TIME_BIN_PRECISION = 8;

  public static final TimePeriod DEFAULT_TIME_PERIOD = TimePeriod.DAY;
}
