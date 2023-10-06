package cn.edu.whu.trajspark.core.conf.store;

import cn.edu.whu.trajspark.core.enums.StoreSchemaEnum;
import cn.edu.whu.trajspark.database.load.type.TextSplitType;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.NotImplementedError;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Xu Qi
 * @since 2023/1/1
 */
public class HBaseStoreConfig implements IStoreConfig {

  private final String location;
  private final String dataSetName;
  private final IndexType mainIndex;
  private String otherIndex;
  private final StoreSchemaEnum schema;
  private final List<IndexMeta> indexList;
  private final DataSetMeta dataSetMeta;

  @JsonCreator
  public HBaseStoreConfig(
      @JsonProperty("location") String location,
      @JsonProperty("dataSetName") String dataSetName,
      @JsonProperty("schema") StoreSchemaEnum schema,
      @JsonProperty("mainIndex") IndexType mainIndex,
      @JsonProperty("otherIndex") @JsonInclude(JsonInclude.Include.NON_NULL) String otherIndex) {
    this.location = location;
    this.dataSetName = dataSetName;
    this.schema = schema;
    this.mainIndex = mainIndex;
    this.otherIndex = otherIndex;
    this.indexList = createIndexList();
    this.dataSetMeta = new DataSetMeta(this.dataSetName, this.indexList);
  }

  @Override
  public StoreType getStoreType() {
    return StoreType.HBASE;
  }

  public String getLocation() {
    return location;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public IndexType getMainIndex() {
    return mainIndex;
  }

  public String getOtherIndex() {
    return otherIndex;
  }

  public StoreSchemaEnum getSchema() {
    return schema;
  }

  public List<IndexMeta> getIndexList() {
    return indexList;
  }

  public DataSetMeta getDataSetMeta() {
    return dataSetMeta;
  }

  public void setOtherIndex(String otherIndex) {
    this.otherIndex = otherIndex;
  }

  private List<IndexMeta> createIndexList() {
    List<IndexMeta> indexMetaList = new LinkedList<>();
    IndexMeta mainIndexMeta = createIndexMeta(mainIndex, true);
    indexMetaList.add(mainIndexMeta);
    if (otherIndex != null) {
      List<IndexMeta> otherIndexMeta = createOtherIndex(otherIndex, TextSplitType.CSV);
      indexMetaList.addAll(otherIndexMeta);
    }
    checkIndexMeta(indexMetaList, mainIndexMeta);
    return indexMetaList;
  }
  private void checkIndexMeta(List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta){
    // 检查重复
    HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
    if (hashSet.size() != indexMetaList.size()) {
      throw new IllegalArgumentException("found duplicate index meta in the list.");
    }
  }

  private IndexMeta createIndexMeta(IndexType indexType, Boolean isMainIndex) {
    switch (indexType) {
      case XZ2:
        return new IndexMeta(isMainIndex, new XZ2IndexStrategy(), dataSetName, "default");
      default:
        throw new NotImplementedError();
    }
  }

  private List<IndexMeta> createOtherIndex(String otherIndex, TextSplitType splitType) {
    String[] indexValue = otherIndex.split(splitType.getDelimiter());
    ArrayList<IndexMeta> indexMetaList = new ArrayList<>();
    for (String index : indexValue) {
      IndexType indexType = IndexType.valueOf(index);
      IndexMeta indexMeta = createIndexMeta(indexType, false);
      indexMetaList.add(indexMeta);
    }
    return indexMetaList;
  }

  @Override
  public String toString() {
    return "HBaseStoreConfig{" +
        "mainIndex=" + mainIndex +
        ", otherIndex='" + otherIndex + '\'' +
        ", dataSetMeta=" + dataSetMeta +
        '}';
  }
}
