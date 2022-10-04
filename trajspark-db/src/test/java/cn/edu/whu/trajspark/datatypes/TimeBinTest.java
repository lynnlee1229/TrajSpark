package cn.edu.whu.trajspark.datatypes;

import junit.framework.TestCase;

/**
 * @author Haocheng Wang
 * Created on 2022/10/2
 */
public class TimeBinTest extends TestCase {

  public void testGetBinStartTime() {
    TimeBin bin = new TimeBin(2000, TimePeriod.WEEK);
    System.out.println(bin.getBinStartTime());
  }

  public void testGetBinEndTime() {
    TimeBin bin = new TimeBin(2000, TimePeriod.WEEK);
    System.out.println(bin.getBinEndTime());
  }
}