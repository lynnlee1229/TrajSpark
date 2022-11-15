package cn.edu.whu.trajspark.coding.sfc;

import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.WKTWriter;

import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_XZ2_PRECISION;
import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/11/14
 */
public class XZ2SFCTest extends TestCase {

  public void testCode() {
    Trajectory t = getExampleTrajectory();
    Envelope boundingBox = t.getLineString().getEnvelopeInternal();
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    org.locationtech.geomesa.curve.XZ2SFC xz2SfcGeomesa = org.locationtech.geomesa.curve.XZ2SFC.apply(MAX_XZ2_PRECISION);
    // 4854295219L
    assert 4854295219L == xz2SfcWhu.index(minLng, maxLng, minLat, maxLat, false);
    assert 4854295219L == xz2SfcGeomesa.index(minLng, minLat, maxLng, maxLat, false);
  }


  public void testRevertToSequenceList() {
    Trajectory t = getExampleTrajectory();

    List<Integer> list = XZ2SFC.getInstance(MAX_XZ2_PRECISION).getQuadrantSequence(4854295219L);
    long v2 = 0L;
    for (int i = 0; i < list.size(); i++) {
      v2 += 1L + list.get(i) *  ((long) Math.pow(4, MAX_XZ2_PRECISION - i) - 1L) / 3L;
    }
    assert 4854295219L == v2;
  }


  public void testGetEnlargedRegion() {

  }

  public void testGetQuadRegion() {
  }
}