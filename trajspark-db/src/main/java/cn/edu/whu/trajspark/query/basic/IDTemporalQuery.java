package cn.edu.whu.trajspark.query.basic;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.query.coprocessor.STCoprocessorQuery;
import cn.edu.whu.trajspark.query.coprocessor.autogenerated.QueryCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Xu Qi
 * @since 2022/11/10
 */
public class IDTemporalQuery extends AbstractQuery {

  TemporalQueryCondition temporalQueryCondition;
  IDQueryCondition idQueryCondition;

  public IDTemporalQuery(DataSet dataSet, TemporalQueryCondition temporalQueryCondition,
                         IDQueryCondition idQueryCondition) throws IOException {
    super(dataSet);
    this.temporalQueryCondition = temporalQueryCondition;
    this.idQueryCondition = idQueryCondition;
  }

  public IDTemporalQuery(IndexTable indexTable, TemporalQueryCondition temporalQueryCondition,
                         IDQueryCondition idQueryCondition) throws IOException {
    super(indexTable);
    this.temporalQueryCondition = temporalQueryCondition;
    this.idQueryCondition = idQueryCondition;
  }

  @Override
  public List<RowKeyRange> getIndexRanges() throws IOException {
    setupTargetIndexTable();
    return targetIndexTable.getIndexMeta().getIndexStrategy().getScanRanges(temporalQueryCondition, idQueryCondition.getMoid());
  }

  @Override
  public List<Trajectory> executeQuery() throws IOException {
    List<RowKeyRange> rowKeyRanges = getIndexRanges();
    return executeQuery(rowKeyRanges);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
    List<QueryCondition.Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
    List<QueryCondition.TemporalQueryWindow> temporalQueryWindows = buildProtoTemporalWindows(temporalQueryCondition);

    QueryCondition.QueryRequest timeQueryRequest = QueryCondition.QueryRequest.newBuilder()
        .setTemporalQueryType(
            temporalQueryCondition.getTemporalQueryType() == TemporalQueryType.CONTAIN
                ? QueryCondition.QueryType.CONTAIN : QueryCondition.QueryType.INTERSECT)
        .setTemporalQueryWindows(
            QueryCondition.TemporalQueryWindows.newBuilder().addAllTemporalQueryWindow(temporalQueryWindows)
                .build())
        .setOid(idQueryCondition.getMoid())
        .addAllRange(ranges).build();

    return STCoprocessorQuery.executeQuery(targetIndexTable, timeQueryRequest);
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    // find a spatial index
    List<IndexMeta> spatialIndexList = null;
    if (map.containsKey(IndexType.OBJECT_ID_T)) {
      spatialIndexList = map.get(IndexType.OBJECT_ID_T);
    }
    if (spatialIndexList != null) {
      return IndexMeta.getBestIndexMeta(spatialIndexList);
    }
    // no spatial index so we will do a full table scan, we select a main index.
    return dataSet.getDataSetMeta().getCoreIndexMeta();
  }

  @Override
  public String getQueryInfo() {
    return temporalQueryCondition.getConditionInfo() + ", " + idQueryCondition.getConditionInfo();
  }

  public static List<QueryCondition.TemporalQueryWindow> buildProtoTemporalWindows(TemporalQueryCondition temporalQueryCondition) {
    List<QueryCondition.TemporalQueryWindow> temporalQueryWindows = new ArrayList<>();
      for (TimeLine queryWindow : temporalQueryCondition.getQueryWindows()) {
        QueryCondition.TemporalQueryWindow temporalQueryWindow = QueryCondition.TemporalQueryWindow.newBuilder()
            .setStartMs(queryWindow.getTimeStart().toEpochSecond())
            .setEndMs(queryWindow.getTimeEnd().toEpochSecond()).build();
        temporalQueryWindows.add(temporalQueryWindow);
      }
    return temporalQueryWindows;
  }
}
