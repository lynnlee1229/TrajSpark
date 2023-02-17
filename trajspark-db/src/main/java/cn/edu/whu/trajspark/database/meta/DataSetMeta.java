package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class DataSetMeta {

  private static Logger logger = LoggerFactory.getLogger(DataSetMeta.class);

  String dataSetName;
  List<IndexMeta> indexMetaList;
  IndexMeta coreIndexMeta;
  String desc = "";

  /**
   * 将默认List中的第一项为core index.
   * @param dataSetName
   * @param indexMetaList
   */
  public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList) {
    this(dataSetName, indexMetaList, getIndexMetaByName(indexMetaList, indexMetaList.get(0).getCoreIndexTableName()));
  }

  public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta) {
    check(indexMetaList, coreIndexMeta);
    this.dataSetName = dataSetName;
    this.indexMetaList = indexMetaList;
    this.coreIndexMeta = coreIndexMeta;
  }

  public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta, String desc) {
    this(dataSetName, indexMetaList, coreIndexMeta);
    this.desc = desc;
  }

  public String getDesc() {
    return desc;
  }

  public IndexMeta getCoreIndexMeta() {
    return coreIndexMeta;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public List<IndexMeta> getIndexMetaList() {
    return indexMetaList;
  }

  public Map<IndexType, List<IndexMeta>> getAvailableIndexes() {
    List<IndexMeta> indexMetaList = getIndexMetaList();
    HashMap<IndexType, List<IndexMeta>> map = new HashMap<>();
    for (IndexMeta indexMeta : indexMetaList) {
      map.put(indexMeta.getIndexStrategy().getIndexType(), Arrays.asList(indexMeta));
    }
    return map;
  }

  @Override
  public String toString() {
    return "DataSetMeta{" +
        "dataSetName='" + dataSetName + '\'' +
        ", coreIndexMeta=" + coreIndexMeta +
        ", indexMetaList=" + indexMetaList +
        ", desc='" + desc + '\'' +
        '}';
  }

  private void check(List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta) throws IllegalArgumentException {
    // 检查重复
    // TODO: 没有必要，indexMetaList应该替换用set类型。
    HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
    if (hashSet.size() != indexMetaList.size()) {
      throw new IllegalArgumentException("found duplicate index meta in the list.");
    }
    // 确认coreIndex存在
    if (coreIndexMeta == null) {
      throw new IllegalArgumentException(String.format("Index meta didn't set core index."));
    }
    // 确认各index meta的coreIndex一致
    String coreTableName = coreIndexMeta.getIndexTableName();
    boolean coreIndexInList = false;
    for (IndexMeta indexMeta : indexMetaList) {
      String curCoreTableName = indexMeta.getCoreIndexTableName();
      if (indexMeta.getIndexTableName().equals(curCoreTableName)) {
        coreIndexInList = true;
      }
      if (curCoreTableName == null) {
        throw new IllegalArgumentException(String.format("Index meta didn't set core index, index table name: %s", indexMeta.indexTableName));
      } else if (!curCoreTableName.equals(coreTableName)) {
        throw new IllegalArgumentException(String.format("Inconsistent core index meta found: %s and %s", coreTableName, curCoreTableName));
      }
    }
    if (!coreIndexInList) {
      throw new IllegalArgumentException("Core index not found in the index meta list");
    }
  }

  private static IndexMeta getIndexMetaByName(List<IndexMeta> indexMetaList, String tableName) {
    for (IndexMeta im : indexMetaList) {
      if (im.getIndexTableName().equals(tableName)) {
        return im;
      }
    }
    return null;
  }
}
