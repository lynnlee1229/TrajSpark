package cn.edu.whu.trajspark.coding.sfc;

import cn.edu.whu.trajspark.datatypes.TimeBin;

/**
 * @author Xu Qi
 * @since 2022/10/13
 */
public class TimeIndexRange {

  private final long lower;
  private final long upper;
  private final TimeBin timeBin;
  private final short bin;
  private boolean contained;

  public TimeIndexRange(long lower, long upper, TimeBin timeBin, boolean contained) {
    this.lower = lower;
    this.upper = upper;
    this.timeBin = timeBin;
    this.bin = timeBin.getBin();
    this.contained = contained;
  }

  public long getLower() {
    return lower;
  }

  public long getUpper() {
    return upper;
  }

  public short getBin() {
    return bin;
  }

  public TimeBin getTimeBin() {
    return timeBin;
  }

  public boolean isContained() {
    return contained;
  }

  public void setContained(boolean contained) {
    this.contained = contained;
  }

  @Override
  public String toString() {
    return "TimeIndexRange{" + "lower=" + lower + ", upper=" + upper + ", timeBin=" + timeBin
        + ", bin=" + bin + ", contained=" + contained + '}';
  }
}
