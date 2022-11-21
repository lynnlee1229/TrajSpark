package cn.edu.whu.trajspark.coding.poscode;

import cn.edu.whu.trajspark.coding.XZ2PCoding;
import cn.edu.whu.trajspark.coding.poscode.PosCode;
import cn.edu.whu.trajspark.coding.poscode.QuadID;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import junit.framework.TestCase;
import org.locationtech.jts.io.WKTWriter;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/11/12
 */
public class PosCodeTest extends TestCase {

  XZ2PCoding xz2PCoding = new XZ2PCoding();

  public void testGetPosCodeByte() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    ByteArray xz2PCode = xz2PCoding.code(exampleTrajectory.getLineString());
    WKTWriter wktWriter = new WKTWriter();
    // Xz2 code: 4854295219
    // Xz2 wkt : POLYGON ((114.08203125 22.587890625, 114.08203125 22.67578125, 114.2578125 22.67578125, 114.2578125 22.587890625, 114.08203125 22.587890625))
    // Traj wkt: LINESTRING (114.06896 22.542664, 114.08942 22.543316, 114.116684 22.547997, 114.118904 22.562414, 114.10953 22.59049)
    // Pos code: PosCode{poscode=1101}
    assertEquals(xz2PCoding.getPosCode(xz2PCode).getPoscodeByte(), 13);
    assertEquals( wktWriter.write(xz2PCoding.getXz2Sfc().getQuadRegion(xz2PCoding.getXZ2Code(xz2PCode), 3)),
        "POLYGON ((114.08203125 22.587890625, 114.08203125 22.67578125, 114.2578125 22.67578125, 114.2578125 22.587890625, 114.08203125 22.587890625))");
  }


  public void testListPossiblePosCodes() {
    Set<QuadID> set = new HashSet<>();
    System.out.println(PosCode.listPossiblePosCodes(set));
  }

  public void testGetPosCodeRanges() {
    Set<QuadID> list = new HashSet<>();
    for (PosCodeRange range : PosCode.toPosCodeRanges(PosCode.listPossiblePosCodes(list))) {
      System.out.printf("Range start: {%s}, ", range.lower);
      System.out.printf("Range end: {%s}. \n", range.upper);
    }
  }
}