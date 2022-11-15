package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.index.RowKeyRange;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_SUFFIX;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class DataTable {

  private Table table;

  private DataSetMeta dataSetMeta;

  public DataTable(String dataSetName) throws IOException {
    this.dataSetMeta = Database.getInstance().getDataSetMeta(dataSetName);
    this.table = Database.getInstance().getTable(dataSetName + DATA_TABLE_SUFFIX);
  }

  public Table getTable() {
    return table;
  }

  public DataSetMeta getDataSetMeta() {
    return dataSetMeta;
  }

  /**
   * Get a Put object which helps put *one* trajectory into data table.
   * The number of rows is the same as the index metas.
   * The PTR of secondary indexes points to the row key generated by the first main index.
   */
  public void put(Trajectory trajectory) throws IOException {
    Deque<IndexMeta> indexMetaDeque = new LinkedList<>(dataSetMeta.getIndexMetaList());
    boolean hasMainIndexBefore = false;
    byte[] ptr = null;
    while (!indexMetaDeque.isEmpty()) {
      IndexMeta indexMeta = indexMetaDeque.pollFirst();
      Put put = null;
      if (indexMeta.isMainIndex()) {
        put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
        if (!hasMainIndexBefore) {
          hasMainIndexBefore = true;
          ptr = put.getRow();
        }
      } else {
        if (!hasMainIndexBefore) {
          indexMetaDeque.offerLast(indexMeta);
          continue;
        } else {
          put = TrajectorySerdeUtils.getPutForSecondaryIndex(indexMeta, trajectory, ptr);
        }
      }
      table.put(put);
    }
  }

  public Result get(Get get) throws IOException {
    return table.get(get);
  }

  public void delete(Delete delete) throws IOException {
    table.delete(delete);
  }

  public ResultScanner getScanner(Scan scan) throws IOException {
    return table.getScanner(scan);
  }

  public List<Result> scan(RowKeyRange rowKeyRange) throws IOException {
    Scan scan = new Scan();
    scan.withStartRow(rowKeyRange.getStartKey().getBytes());
    scan.withStopRow(rowKeyRange.getEndKey().getBytes());
    List<Result> results = new ArrayList<>();
    for (Result r : table.getScanner(scan)) {
      results.add(r);
    }
    return results;
  }

  public void close() throws IOException {
    table.close();
  }
}
