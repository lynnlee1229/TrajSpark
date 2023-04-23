package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class IndexMeta implements Serializable {

  boolean isMainIndex;
  IndexStrategy indexStrategy;
  String dataSetName;
  String indexTableName;

  public IndexMeta() {
  }

  /**
   * 使用此构造方法创建的IndexMeta本身即为core index
   */
  public IndexMeta(boolean isMainIndex,
                   IndexStrategy indexStrategy,
                   String dataSetName,
                   String tableNameSuffix) {
    this.isMainIndex = isMainIndex;
    this.indexStrategy = indexStrategy;
    this.dataSetName = dataSetName;
    this.indexTableName = dataSetName + "-" + indexStrategy.getIndexType().name() + "-" + tableNameSuffix;
  }

  public IndexType getIndexType() {
    return indexStrategy.getIndexType();
  }

  public boolean isMainIndex() {
    return isMainIndex;
  }

  public IndexStrategy getIndexStrategy() {
    return indexStrategy;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public String getIndexTableName() {
    return indexTableName;
  }

  @Override
  public String toString() {
    return "IndexMeta{" +
            "isMainIndex=" + isMainIndex +
            ", indexStrategy=" + indexStrategy +
            ", dataSetName='" + dataSetName + '\'' +
            ", indexTableName='" + indexTableName + '\'' +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexMeta indexMeta = (IndexMeta) o;
    return isMainIndex == indexMeta.isMainIndex && Objects.equals(indexStrategy, indexMeta.indexStrategy) && Objects.equals(dataSetName, indexMeta.dataSetName) && Objects.equals(indexTableName, indexMeta.indexTableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isMainIndex, indexStrategy, dataSetName, indexTableName);
  }

  /**
   * 从相同IndexType的多个IndexMeta找出最适于查询的IndexMeta.
   * 当前的判定依据为选取其中代表主索引的IndexMeta，如果全部是辅助索引，则取其中的第一个。
   * 未来可加入其他因素，如数据集是否已经清洗、索引的参数，或者结合查询条件选择最佳的索引。
   * @param indexMetaList 相同IndexType的多个IndexMeta
   * @return 最适于查询的IndexMeta
   */
  public static IndexMeta getBestIndexMeta(List<IndexMeta> indexMetaList) {
    for (IndexMeta indexMeta : indexMetaList) {
      if (indexMeta.isMainIndex()) {
        return indexMeta;
      }
    }
    return indexMetaList.get(0);
  }

  public byte[][] getSplits() {
    return indexStrategy.getSplits();
  }
}
