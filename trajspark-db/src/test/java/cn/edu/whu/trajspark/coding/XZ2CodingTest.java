package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.WKTWriter;

/**
 * @author Haocheng Wang
 * Created on 2022/10/5
 */
public class XZ2CodingTest {

  @Test
  public void testGetXZ2Region() {
    Trajectory t = XZ2IndexStrategyTest.getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    WKTWriter wktWriter = new WKTWriter();
    ByteArray spatialCodingByteArray = XZ2IndexStrategy.extractSpatialCode(byteArray);

    assert wktWriter.write(XZ2IndexStrategy.getSpatialCoding().getCodingPolygon(spatialCodingByteArray))
        .equals("POLYGON ((113.90625 22.5, 113.90625 22.67578125, 114.2578125 22.67578125, 114.2578125 22.5, 113.90625 22.5))");
  }
}