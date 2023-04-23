package cn.edu.whu.trajspark.core.util;

import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.index.IndexType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Haocheng Wang
 * Created on 2023/2/8
 */
public class DataBaseUtils {

  static Database instance;

  static {
    try {
      instance = Database.getInstance();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void createDataSet(String datasetName, List<IndexMeta> indexMetaList) {
    // create dataset
    DataSetMeta dataSetMeta = new DataSetMeta(datasetName, indexMetaList);
    try {
      instance.createDataSet(dataSetMeta);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static IndexTable getMainIndexTable(String datasetName) throws IOException {
    return instance.getDataSet(datasetName).getCoreIndexTable();
  }

  public static List<IndexMeta> getIndexMetaList(String datasetName) throws IOException {
    Map<IndexType, List<IndexMeta>> map = instance.getDataSetMeta(datasetName).getAvailableIndexes();
    List<IndexMeta> list = new LinkedList<>();
    for (IndexType indexType : map.keySet()) {
      list.addAll(map.get(indexType));
    }
    return list;
  }
}
