package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.index.IndexType;
import org.locationtech.sfcurve.IndexRange;

import java.io.IOException;
import java.util.*;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public abstract class AbstractQuery {
  DataTable dataTable;

  public AbstractQuery(DataTable dataTable) {
    this.dataTable = dataTable;
  }

  /**
   * 基于查询条件与目标表, 获取要查询的row-key范围.
   * @return
   */
  public abstract List<RowKeyRange> getIndexRanges();

  /**
   * Execute query directly on the target table.
   * @return
   */
  public abstract List<Trajectory> executeQuery() throws IOException;

  /**
   * Query a specific range of target table locally.
   * @return
   */
  public abstract List<Trajectory> executeQuery(List<RowKeyRange> range) throws IOException;

  public abstract IndexMeta findBestIndex();

  protected Map<IndexType, IndexMeta> getAvailableIndexes() {
    DataSetMeta dataSetMeta = dataTable.getDataSetMeta();
    List<IndexMeta> indexMetaList = dataSetMeta.getIndexMetaList();
    HashMap<IndexType, IndexMeta> map = new HashMap<>();
    for (IndexMeta indexMeta : indexMetaList) {
      map.put(indexMeta.getIndexStrategy().getIndexType(), indexMeta);
    }
    return map;
  }
}
