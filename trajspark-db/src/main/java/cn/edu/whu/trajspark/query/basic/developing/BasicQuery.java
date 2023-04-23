package cn.edu.whu.trajspark.query.basic.developing;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * TODO:
 * @author Haocheng Wang
 * Created on 2023/2/16
 */
public class BasicQuery {
  private static Logger logger = LoggerFactory.getLogger(BasicQuery.class);
  private DataSet dataSet;
  private IndexTable targetIndexTable;
  private BasicQueryCondition bqc;

  /**
   * 指定该查询面向的数据集，由DB自行选取要使用的目标索引表
   */
  public BasicQuery(DataSet dataSet, BasicQueryCondition bqc) {
    this.dataSet = dataSet;
    this.bqc = bqc;
  }

  /**
   * 强制指定该查询面向的索引表
   */
  public BasicQuery(IndexTable targetIndexTable, BasicQueryCondition bqc) {
    this.targetIndexTable = targetIndexTable;
    this.bqc = bqc;
  }


  // TODO
  public List<RowKeyRange> getIndexRanges() {
    // setupTargetIndexTable();
    // return targetIndexTable.getIndexMeta().getIndexStrategy().getScanRanges(bqc);
    return null;
  }

  /**
   * Query <strong>all</strong> ranges that meets query request on target table.
   * @return
   */
  public List<Trajectory> executeQuery() {
    try {
      List<RowKeyRange> rowKeyRanges = getIndexRanges();
      return executeQuery(rowKeyRanges);
    } catch (Exception e) {
      logger.error("Basic query failed, ", e);
      return new LinkedList<>();
    }
  }

  /**
   * TODO
   * Query <strong>some</strong> ranges on target table.
   * @return
   */
  public List<Trajectory> executeQuery(List<RowKeyRange> ranges) {
    // setupTargetIndexTable();
    // List<Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
    // QueryRequest spatialTemporalQueryRequest = QueryRequest.newBuilder().build()
    // return STCoprocessorQuery.executeQuery(targetIndexTable, spatialTemporalQueryRequest);
    return null;
  }

  /**
   * 根据当前查询中各维度的约束信息与目标表的索引配置情况，选择目标索引
   * @return
   */
  private IndexMeta findBestIndex() {
    // 用户已自行指定目标索引表
    if (targetIndexTable != null) {
      return targetIndexTable.getIndexMeta();
    }
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    List<IndexType> usableIndexTypes = bqc.usableIndexTypes();
    for (IndexType usableIndexType : usableIndexTypes) {
      if (map.containsKey(usableIndexType)) {
        return IndexMeta.getBestIndexMeta(map.get(usableIndexType));
      }
    }
    return null;
  }

  private void setupTargetIndexTable() throws IOException, IllegalAccessException {
    if (targetIndexTable == null) {
      IndexMeta indexMeta = findBestIndex();
      if (indexMeta == null) {
        throw new IllegalAccessException(
            String.format("无可用于查询[%s]的索引，暂不支持以全表扫描的形式执行查询。", toString()));
      }
      logger.info("Query [{}] will be executed on table: {}", toString(), indexMeta.getIndexTableName());
      targetIndexTable = dataSet.getIndexTable(indexMeta);
    }
  }

  @Override
  public String toString() {
    return "BasicQuery{" +
        "dataSet=" + dataSet +
        ", targetIndexTable=" + targetIndexTable +
        ", bqc=" + bqc +
        '}';
  }
}
