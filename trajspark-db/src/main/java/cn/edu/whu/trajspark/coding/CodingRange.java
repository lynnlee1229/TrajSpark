package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.coding.poscode.PosCode;
import cn.edu.whu.trajspark.coding.poscode.PosCodeRange;
import cn.edu.whu.trajspark.coding.sfc.SFCRange;
import cn.edu.whu.trajspark.datatypes.ByteArray;

import java.nio.ByteBuffer;

/**
 * 编码范围，边界以byte array存储，更通用。
 *
 * @author Haocheng Wang
 * Created on 2022/11/13
 */
public class CodingRange {
  ByteArray lower;
  ByteArray upper;
  boolean contained;

  public CodingRange(ByteArray lower, ByteArray upper, boolean contained) {
    this.lower = lower;
    this.upper = upper;
    this.contained = contained;
  }

  public CodingRange() {
  }

  public ByteArray getLower() {
    return lower;
  }

  public ByteArray getUpper() {
    return upper;
  }

  public boolean isContained() {
    return contained;
  }

  public void concatSfcRange(SFCRange sfcRange) {
    if (lower == null || upper == null) {
      lower = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.upper));
      contained = sfcRange.contained;
    } else {
      lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(lower.getBytes())
          .putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(upper.getBytes())
          .putLong(sfcRange.upper));
      contained = sfcRange.contained;
    }
  }

  public void concatPosCodeRange(PosCodeRange posCodeRange) {
    if (lower == null || upper == null) {
      lower = new ByteArray(ByteBuffer.allocate(PosCode.SIZE / Byte.SIZE).put(posCodeRange.lower.getPoscodeByte()));
      upper = new ByteArray(ByteBuffer.allocate(PosCode.SIZE / Byte.SIZE).put(posCodeRange.upper.getPoscodeByte()));
    } else {
      lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + PosCode.SIZE / Byte.SIZE)
          .put(lower.getBytes())
          .put(posCodeRange.lower.getPoscodeByte()));
      upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + PosCode.SIZE / Byte.SIZE)
          .put(upper.getBytes())
          .put(posCodeRange.upper.getPoscodeByte()));
    }
  }

  @Override
  public String toString() {
    return "CodingRange{" +
        "lower=" + lower +
        ", upper=" + upper +
        ", contained=" + contained +
        '}';
  }
}
