package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.index.RowKeyRange;
import org.apache.hadoop.hbase.client.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * For main index table:
 * <table>
 *   <col width="20%"/>
 *   <col width="80%"/>
 *   <thead>
 *     <tr><th>RowKey</th>
 *     <th>Data(ColumnFamily)</th></tr>
 *   <thead>
 *   <tbody>
 *      <tr><td>index value        |</td><td>mbr|start_point|end_point|point_list|mo_id|traj_id|signature(optional)|</td></tr>
 *   </tbody>
 * </table>
 *
 * <P>
 * For secondary index table:
 *  <table>
 *    <col width="20%"/>
 *    <col width="80%"/>
 *    <thead>
 *      <tr><th>RowKey</th>
 *      <th>Data(ColumnFamily)</th></tr>
 *    <thead>
 *    <tbody>
 *       <tr><td>index value        |</td><td>mbr|start_point|end_point|prt|</td></tr>
 *    </tbody>
 *  </table>
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class IndexTable {

  private Table table;

  private IndexMeta indexMeta;

  public IndexTable(IndexMeta indexMeta) throws IOException {
    this.indexMeta = indexMeta;
    this.table = Database.getInstance().getTable(indexMeta.getIndexTableName());
  }

  public IndexTable(String tableName) throws IOException {
    this.indexMeta = Database.getInstance().getDataSet(extractDataSetName(tableName)).getIndexTable(tableName).getIndexMeta();
    this.table = Database.getInstance().getTable(tableName);
  }

  public IndexMeta getIndexMeta() {
    return indexMeta;
  }

  public Table getTable() {
    return table;
  }

  /**
   * 将轨迹put至主表所使用的方法。若该数据集有多份索引，则put至非主表的各索引表的工作由主表上的Observer协处理器完成。
   */
  public void putForMainTable(Trajectory trajectory) throws IOException {
      table.put(TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory));
  }

  public void putForSecondaryTable(Trajectory trajectory, byte[] ptr) throws IOException {
    Put put = TrajectorySerdeUtils.getPutForSecondaryIndex(indexMeta, trajectory, ptr, false);
    table.put(put);
  }

  public void put(Trajectory trajectory, @Nullable byte[] ptr) throws IOException {
    if (indexMeta.isMainIndex()) {
      putForMainTable(trajectory);
    } else {
      putForSecondaryTable(trajectory, ptr);
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

  // 表名结构: DataSetName-IndexType-Suffix
  public static String extractDataSetName(IndexTable indexTable) {
    String tableName = indexTable.getTable().getName().getNameAsString();
    return extractDataSetName(tableName);
  }

  // 表名结构: DataSetName-IndexType-Suffix
  public static String extractDataSetName(String tableName) {
    String[] strs = tableName.split("-");
    String indexTypeStr = strs[strs.length - 2];
    String suffix = strs[strs.length - 1];
    return tableName.substring(0, tableName.length() - indexTypeStr.length() - suffix.length() - 2);
  }
}
