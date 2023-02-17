package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/9
 */
public class DataSet {

  private DataSetMeta dataSetMeta;

  public DataSet(DataSetMeta dataSetMeta) {
    this.dataSetMeta = dataSetMeta;
  }

  public IndexTable getCoreIndexTable() throws IOException {
    return new IndexTable(dataSetMeta.getCoreIndexMeta());
  }

  public IndexTable getIndexTable(IndexMeta indexMeta) throws IOException {
    return new IndexTable(indexMeta);
  }

  public DataSetMeta getDataSetMeta() {
    return dataSetMeta;
  }
}
