package cn.edu.whu.trajspark.datatypes;

/**
 * @author Xu Qi
 * @since 2022/10/13
 */
public class TimeIndexRange {

  private final long lower;
  private final long upper;
  private final TimeBin timeBin;
  private final short bin;

  public TimeIndexRange(long lower, long upper, TimeBin timeBin) {
    this.lower = lower;
    this.upper = upper;
    this.timeBin = timeBin;
    this.bin = timeBin.getBin();
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

  @Override
  public String toString() {
    return "TimeIndexRange{" + "lower=" + lower + ", upper=" + upper + ", timeBin=" + timeBin + '}';
  }
}
