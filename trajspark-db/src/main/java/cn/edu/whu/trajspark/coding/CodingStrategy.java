package cn.edu.whu.trajspark.coding;

/**
 * 接收对象,输出row-key.
 * 接收row-key,输出索引值
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public abstract class CodingStrategy {
  CodingType codingType;
  /**
   * Get index type of the index;
   * @return Index type of the index object.
   */
  public CodingType getIndexType() {
    return codingType;
  }
}
