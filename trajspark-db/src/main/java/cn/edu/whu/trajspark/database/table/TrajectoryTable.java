package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.index.IndexStrategy;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class TrajectoryTable {
  private Table table;
  private IndexStrategy indexStrategy;

  public TrajectoryTable(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  public void put(Put put) throws IOException {
    table.put(put);
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

  public List<Result> scan(Scan scan) throws IOException {
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
