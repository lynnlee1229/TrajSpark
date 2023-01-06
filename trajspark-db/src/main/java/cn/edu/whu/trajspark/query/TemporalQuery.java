package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.query.coprocessor.CoprocessorQuery;
import cn.edu.whu.trajspark.query.coprocessor.autogenerated.QueryCondition.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Xu Qi
 * @since 2022/11/10
 */
public class TemporalQuery extends AbstractQuery {

  TemporalQueryCondition temporalQueryCondition;
  String oID;

  public TemporalQuery(DataTable dataTable) {
    super(dataTable);
  }

  public TemporalQuery(DataTable dataTable, TemporalQueryCondition temporalQueryCondition,
      String oID) {
    super(dataTable);
    this.temporalQueryCondition = temporalQueryCondition;
    this.oID = oID;
  }

  @Override
  public List<RowKeyRange> getIndexRanges() {
    IndexMeta indexMeta = findBestIndex();
    return indexMeta.getIndexStrategy().getScanRanges(temporalQueryCondition, oID);
  }

  @Override
  public List<Trajectory> executeQuery() throws IOException {
    List<RowKeyRange> rowKeyRanges = getIndexRanges();
    return executeQuery(rowKeyRanges);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
    List<Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
    List<TemporalQueryWindow> temporalQueryWindows = new ArrayList<>();
    if (temporalQueryCondition.getQueryWindows() == null) {
      TemporalQueryWindow window = TemporalQueryWindow.newBuilder()
          .setStartMs(temporalQueryCondition.getQueryWindow().getTimeStart().toEpochSecond())
          .setEndMs(temporalQueryCondition.getQueryWindow().getTimeEnd().toEpochSecond()).build();
      temporalQueryWindows.add(window);
    } else {
      for (TimeLine queryWindow : temporalQueryCondition.getQueryWindows()) {
        TemporalQueryWindow temporalQueryWindow = TemporalQueryWindow.newBuilder()
            .setStartMs(queryWindow.getTimeStart().toEpochSecond())
            .setEndMs(queryWindow.getTimeEnd().toEpochSecond()).build();
        temporalQueryWindows.add(temporalQueryWindow);
      }
    }

    QueryRequest timeQueryRequest = QueryRequest.newBuilder()
        .setTemporalQueryType(
            temporalQueryCondition.getTemporalQueryType() == TemporalQueryType.CONTAIN
                ? QueryType.CONTAIN : QueryType.INTERSECT)
        .setTemporalQueryWindows(
            TemporalQueryWindows.newBuilder().addAllTemporalQueryWindow(temporalQueryWindows)
                .build())
        .setOid(oID)
        .addAllRange(ranges).build();

    return CoprocessorQuery.executeQuery(dataTable, timeQueryRequest);
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, IndexMeta> map = getAvailableIndexes();
    // find a spatial index
    if (map.containsKey(IndexType.OBJECT_ID_T)) {
      return map.get(IndexType.OBJECT_ID_T);
    }
    for (Map.Entry<IndexType, IndexMeta> entry : map.entrySet()) {
      if (entry.getValue().isMainIndex()) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Override
  protected Map<IndexType, IndexMeta> getAvailableIndexes() {
    return super.getAvailableIndexes();
  }
}
