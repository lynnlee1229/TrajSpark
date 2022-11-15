package cn.edu.whu.trajspark.coding.sfc;

/**
 * lower -> contained;
 * upper -> contained.
 * @author Haocheng Wang
 * Created on 2022/11/2
 */
public class SFCRange implements Comparable<SFCRange>{
  public long lower;
  public long upper;
  public boolean contained;

  public SFCRange(long lower, long upper, boolean contained) {
    this.lower = lower;
    this.upper = upper;
    this.contained = contained;
  }

  public static SFCRange apply(long lower, long upper, boolean contained) {
    if (contained) {
      return new CoveredSFCRange(lower, upper);
    } else {
      return new OverlapSFCRange(lower, upper);
    }
  }


  @Override
  public int compareTo(SFCRange o) {
    int c1 = Long.compare(lower, o.lower);
    if (c1 != 0) {
      return c1;
    }
    return Long.compare(upper, o.upper);
  }
}

