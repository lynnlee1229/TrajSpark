package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.spatial.SpatialIndexStrategy;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

import static cn.edu.whu.trajspark.index.spatial.SpatialIndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/10/5
 */
public class XZ2PlusCodingTest {

  @Test
  public void getSequence() {
    XZ2PlusCoding xz2PlusCoding = new XZ2PlusCoding((short) 16);
    Trajectory t = getExampleTrajectory();
    SpatialIndexStrategy spatialIndexStrategy = new SpatialIndexStrategy(new XZ2PlusCoding(), (short) 0);
    ByteArray byteArray = spatialIndexStrategy.index(t);
    long v1 = spatialIndexStrategy.getSpatialCodingVal(byteArray);
    List<Integer> list = xz2PlusCoding.getSequence(v1);
    long v2 = 0L;
    for (int i = 0; i < list.size(); i++) {
      v2 += 1L + list.get(i) *  ((long) Math.pow(4, 16 - i) - 1L) / 3L;
    }
    assert v1 == v2;
  }
}