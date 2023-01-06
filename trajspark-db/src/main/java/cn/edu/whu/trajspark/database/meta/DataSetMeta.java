package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.index.IndexType;
import java.util.Deque;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.META_TABLE_COLUMN_FAMILY;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class DataSetMeta {

  String dataSetName;
  List<IndexMeta> indexMetaList;

  public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList) {
    this.dataSetName = dataSetName;
    assert check(indexMetaList);
    this.indexMetaList = indexMetaList;
    sortByMainIndexMeta();
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public List<IndexMeta> getIndexMetaList() {
    return indexMetaList;
  }

  /**
   * Convert DataSetMeta object into HBase Put object for dataset creation.
   */
  public Put toPut() throws IOException {
    // row key
    Put p = new Put(Bytes.toBytes(dataSetName));
    HashSet<IndexType> indexTypeHashSet = new HashSet<>();
    // Duplicated index type.
    for (IndexMeta indexMeta : indexMetaList) {
      IndexType curType = indexMeta.indexStrategy.getIndexType();
      if (indexTypeHashSet.contains(curType)) {
        throw new IllegalArgumentException("Duplicated index type found in data set meta: " + this);
      }
      int indexTypeId = curType.getId();
      indexTypeHashSet.add(curType);
      p.addColumn(Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          Bytes.toBytes(indexTypeId),
          IndexMeta.serialize(indexMeta));
    }
    return p;
  }

  public static DataSetMeta initFromResult(Result result) {
    byte[] CF = Bytes.toBytes(META_TABLE_COLUMN_FAMILY);
    List<IndexMeta> indexMetaList = new LinkedList<>();
    String dataSetName = new String(result.getRow());
    // every index meta's column qualifier equals to index type name
    for (IndexType indexType : IndexType.values()) {
      byte[] CQ = Bytes.toBytes(indexType.getId());
      Cell cell = result.getColumnLatestCell(CF, CQ);
      if (cell != null) {
        IndexMeta indexMeta = IndexMeta.deserialize(CellUtil.cloneValue(cell));
        indexMetaList.add(indexMeta);
      }
    }
    return new DataSetMeta(dataSetName, indexMetaList);
  }

  public void sortByMainIndexMeta() {
    Deque<IndexMeta> indexMetaDeque = new LinkedList<>(indexMetaList);
    while (!indexMetaDeque.isEmpty()) {
      IndexMeta indexMeta = indexMetaDeque.pollFirst();
      if (indexMeta.isMainIndex()) {
        indexMetaDeque.offerFirst(indexMeta);
        return;
      } else {
        indexMetaDeque.offerLast(indexMeta);
      }
    }
  }

  @Override
  public String toString() {
    return "DataSetMeta{" +
        "dataSetName='" + dataSetName + '\'' +
        ", indexMetaList=" + indexMetaList +
        '}';
  }

  /**
   * At least one main index and no duplicated index type
   */
  private boolean check(List<IndexMeta> indexMetaList) {
    boolean res = false;
    HashSet<IndexType> hashSet = new HashSet<>();
    for (IndexMeta indexMeta : indexMetaList) {
      IndexType indexType = indexMeta.indexStrategy.getIndexType();
      if (hashSet.contains(indexType)) {
        return false;
      }
      hashSet.add(indexType);
      if (indexMeta.isMainIndex) {
        res = true;
      }
    }
    return res;
  }
}
