package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import org.junit.Test;

import java.util.List;

import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/10/5
 */
public class XZ2CodingTest {

  @Test
  public void getSequence() {
    XZ2Coding xz2Coding = new XZ2Coding((short) 16);
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    long v1 = XZ2IndexStrategy.getSpatialCodingVal(byteArray);
    List<Integer> list = xz2Coding.getSequence(v1);
    long v2 = 0L;
    for (int i = 0; i < list.size(); i++) {
      v2 += 1L + list.get(i) *  ((long) Math.pow(4, 16 - i) - 1L) / 3L;
    }
    assert v1 == v2;
  }
}