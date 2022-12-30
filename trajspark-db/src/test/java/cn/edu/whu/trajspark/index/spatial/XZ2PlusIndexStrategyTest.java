package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.coding.XZ2PCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import junit.framework.TestCase;

/**
 * @author Haocheng Wang
 * Created on 2022/11/16
 */
public class XZ2PlusIndexStrategyTest extends TestCase {

  XZ2PlusIndexStrategy xz2PIS = new XZ2PlusIndexStrategy();

  public void testIndex() {
    Trajectory trajectory = XZ2IndexStrategyTest.getExampleTrajectory();
    assertTrue(xz2PIS.parseIndex2String(xz2PIS.index(trajectory)).contains("indexId=XZ2Plus, xz2P=000000012156aab30d, oidTid=001Trajectory"));
  }

  public void testGetScanRanges() {
  }

}