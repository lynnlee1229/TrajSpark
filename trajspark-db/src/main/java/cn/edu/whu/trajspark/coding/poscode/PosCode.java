package cn.edu.whu.trajspark.coding.poscode;

import cn.edu.whu.trajspark.coding.sfc.XZ2SFC;
import cn.edu.whu.trajspark.constant.CodingConstants;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.util.*;

/**
 * @author Haocheng Wang
 * Created on 2022/11/7
 */
public class PosCode implements Comparable<PosCode> {

  private byte poscodeByte;
  private final XZ2SFC XZ2 = XZ2SFC.getInstance(CodingConstants.MAX_XZ2_PRECISION);

  public static int SIZE = 8;

  public PosCode() {
    this.poscodeByte = 0;
  }

  /**
   *
   * @param xz2Sequence
   * @param lineString
   * @return pos code about the line string on xz2 region.
   */
  public PosCode(long xz2Sequence, LineString lineString) {
    for (int quadID = 0; quadID < 4; quadID++) {
      Polygon quadRegion = XZ2.getQuadRegion(xz2Sequence, quadID);
      if (lineString.intersects(quadRegion)) {
        this.setPosition(new QuadID(quadID));
      }
    }
  }

  public byte getPoscodeByte() {
    return poscodeByte;
  }

  public void setPosition(QuadID quadID) {
    poscodeByte = (byte) (poscodeByte | (1 << (3 - quadID.quadID)));
  }

  public PosCode(byte poscodeByte) {
    this.poscodeByte = poscodeByte;
  }

  @Override
  public String toString() {
    return "PosCode{" +
        "poscode=" + Integer.toBinaryString(poscodeByte) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PosCode posCode = (PosCode) o;
    return poscodeByte == posCode.poscodeByte;
  }

  @Override
  public int hashCode() {
    return Objects.hash(poscodeByte);
  }


  /**
   * 获取当PosCode的某些bit为1时，所有可能的PosCode.
   * @param quadIDS
   * @return
   */
  public static Set<PosCode> listPossiblePosCodes(Set<QuadID> quadIDS) {
    // Left bottom(0) will always be true.
    boolean[] flags = {true, false, false, false};
    List<PosCode> res = new LinkedList<>();
    PosCode basicPosCode = new PosCode();
    basicPosCode.setPosition(QuadID.LEFT_BOTTOM_ID);
    for (QuadID quadID : quadIDS) {
      flags[quadID.quadID] = true;
      basicPosCode.setPosition(quadID);
    }
    int pos = 0;
    res.add(new PosCode(basicPosCode.getPoscodeByte()));
    while (pos < 4) {
      if (!flags[pos]) {
        int size = res.size();
        for (int j = 0; j < size; j++) {
          PosCode p = res.get(j);
          PosCode copy = new PosCode(p.getPoscodeByte());
          copy.setPosition(new QuadID(pos));
          res.add(copy);
        }
      }
      pos++;
    }
    if (res.size() == 1) {
      res.clear();
    }
    return new HashSet<>(res);
  }

  /**
   *
   * @param posCodesSet
   * @return Pos code ranges, each range's left and right are included.
   */
  public static List<PosCodeRange> toPosCodeRanges(Set<PosCode> posCodesSet) {
    List<PosCode> posCodes = new ArrayList<>(posCodesSet);
    Collections.sort(posCodes);
    List<PosCodeRange> ranges = new LinkedList<>();
    PosCodeRange range = new PosCodeRange();
    for (PosCode posCode : posCodes) {
      byte posCodeByte = posCode.poscodeByte;
      if (range.lower == null) {
        range.lower = new PosCode(posCodeByte);
        range.upper = new PosCode(posCodeByte);
      } else {
        if (range.upper.poscodeByte + 1 == posCodeByte) {
          range.upper = new PosCode(posCodeByte);
        } else {
          ranges.add(range);
          range = new PosCodeRange();
          range.lower = new PosCode(posCodeByte);
          range.upper = new PosCode(posCodeByte);
        }
      }
    }
    ranges.add(range);
    return ranges;
  }

  @Override
  public int compareTo(PosCode o) {
    return this.poscodeByte - o.poscodeByte;
  }
}