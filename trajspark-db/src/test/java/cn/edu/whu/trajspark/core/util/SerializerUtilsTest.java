package cn.edu.whu.trajspark.core.util;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;

import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class SerializerUtilsTest extends TestCase {

  public void testSerializeList() throws IOException {
    Trajectory t = getExampleTrajectory();
    byte[] bytes = SerializerUtils.serializeList(t.getPointList(), TrajPoint.class);
    List list = SerializerUtils.deserializeList(bytes, TrajPoint.class);
    for (int i = 0; i < list.size(); i++) {
      assert list.get(i).equals(t.getPointList().get(i));
    }
  }

  public void testTrajPointSerialize() throws IOException {
    Trajectory t = getExampleTrajectory();
    TrajPoint tp = t.getPointList().get(0);
    byte[] bytes = SerializerUtils.serializeObject(tp);
    TrajPoint tp2 = (TrajPoint) SerializerUtils.deserializeObject(bytes, TrajPoint.class);
    assert tp.equals(tp2);
  }
}