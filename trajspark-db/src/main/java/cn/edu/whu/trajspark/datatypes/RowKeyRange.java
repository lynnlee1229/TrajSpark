package cn.edu.whu.trajspark.datatypes;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class RowKeyRange {
  ByteArray startKey;
  ByteArray endKey;

  public RowKeyRange(ByteArray startKey, ByteArray endKey) {
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public ByteArray getStartKey() {
    return startKey;
  }

  public ByteArray getEndKey() {
    return endKey;
  }

  @Override
  public String toString() {
    return "RowKeyRange{" +
        "startKey=" + startKey +
        ", endKey=" + endKey +
        '}';
  }
}
