package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.database.table.DataTable;
import org.locationtech.sfcurve.IndexRange;

import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public abstract class Query {
  DataTable targetTable;

  public Query(DataTable targetTable) {
    this.targetTable = targetTable;
  }

  /**
   * Get row-key ranges for further distributed query
   * @return
   */
  abstract List<IndexRange> getIndexRanges();

  /**
   * Execute query directly on the target table.
   * @return
   */
  abstract List<Object> executeQuery();

  /**
   * Query a specific range of target table locally.
   * @return
   */
  abstract List<Object> executeQuery(IndexRange range);
}
