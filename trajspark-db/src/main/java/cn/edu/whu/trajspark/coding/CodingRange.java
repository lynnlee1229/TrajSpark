package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.coding.sfc.SFCRange;
import cn.edu.whu.trajspark.datatypes.ByteArray;

import java.nio.ByteBuffer;

/**
 * 编码范围，边界以byte array存储，更通用。
 *
 * @author Haocheng Wang Created on 2022/11/13
 */
public class CodingRange {

  ByteArray lower;
  ByteArray upper;
  boolean validated;

  public CodingRange(ByteArray lower, ByteArray upper, boolean validated) {
    this.lower = lower;
    this.upper = upper;
    this.validated = validated;
  }

  public CodingRange() {
  }

  public ByteArray getLower() {
    return lower;
  }

  public ByteArray getUpper() {
    return upper;
  }

  public boolean isValidated() {
    return validated;
  }

  public void concatSfcRange(SFCRange sfcRange) {
    if (lower == null || upper == null) {
      lower = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.upper));
      validated = sfcRange.validated;
    } else {
      lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(lower.getBytes())
          .putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(upper.getBytes())
          .putLong(sfcRange.upper));
      validated = sfcRange.validated;
    }
  }

  @Override
  public String toString() {
    return "CodingRange{" +
        "lower=" + lower +
        ", upper=" + upper +
        ", contained=" + validated +
        '}';
  }
}
