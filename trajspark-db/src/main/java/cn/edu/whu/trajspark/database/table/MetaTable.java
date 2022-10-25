package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/11
 */
public class MetaTable {

  private Table dataSetMetaTable;

  public MetaTable(Table dataSetMetaTable) {
    this.dataSetMetaTable = dataSetMetaTable;
  }

  public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
    if (!dataSetExists(dataSetName)) {
      return null;
    } else {
      Result result = dataSetMetaTable.get(new Get(dataSetName.getBytes()));
      return DataSetMeta.initFromResult(result);
    }
  }

  public boolean dataSetExists(String datasetName) throws IOException {
    return dataSetMetaTable.exists(new Get(datasetName.getBytes()));
  }

  public void putDataSet(DataSetMeta dataSetMeta) throws IOException {
    dataSetMetaTable.put(dataSetMeta.toPut());
  }


  // TODO
  public List<DataSetMeta> listDataSet() throws IOException {
    return null;
  }

}
