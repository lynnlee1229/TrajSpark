package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.coprocessor.autogenerated.BasicSTQueryCoprocessor.*;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.*;

/**
 * @author Haocheng Wang
 * Created on 2022/10/27
 */
public class SpatialQuery extends AbstractQuery {

  SpatialQueryCondition spatialQueryCondition;

  public SpatialQuery(DataTable targetTable, SpatialQueryCondition spatialQueryCondition) {
    super(targetTable);
    this.spatialQueryCondition = spatialQueryCondition;
  }

  @Override
  public List<RowKeyRange> getIndexRanges() {
    IndexMeta indexMeta = findBestIndex();
    return indexMeta.getIndexStrategy().getScanRanges(spatialQueryCondition);
  }

  @Override
  public List<Trajectory> executeQuery() throws IOException {
    List<RowKeyRange> rowKeyRanges = getIndexRanges();
    return executeQuery(rowKeyRanges);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {

    List<Range> ranges = new ArrayList<>();
    for (RowKeyRange rowKeyRange : rowKeyRanges) {
      Range r = Range.newBuilder()
          .setStart(ByteString.copyFrom(rowKeyRange.getStartKey().getBytes()))
          .setEnd(ByteString.copyFrom(rowKeyRange.getEndKey().getBytes()))
          .build();
      ranges.add(r);
    }

    ranges.sort((o1, o2) -> {
      ByteArray o1Start = new ByteArray(o1.getStart().toByteArray());
      ByteArray o2Start = new ByteArray(o2.getStart().toByteArray());
      ByteArray o1End = new ByteArray(o1.getEnd().toByteArray());
      ByteArray o2End = new ByteArray(o2.getEnd().toByteArray());
      return o1Start.compareTo(o2Start) == 0 ? o1End.compareTo(o2End) : o1Start.compareTo(o2Start);
    });

    QueryRequest request = QueryRequest.newBuilder()
        .setSpatialQueryType(spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN ? QueryType.CONTAIN : QueryType.INTERSECT)
        .setSpatialQueryWindow(SpatialQueryWindow.newBuilder().setWkt(spatialQueryCondition.getQueryWindowWKT()))
        .addAllRange(ranges).build();

    Map<byte[], List<TrajectoryResult>> coprocessorResult = null;
    try {
      coprocessorResult = dataTable.getTable().coprocessorService(QueryService.class,
          ranges.get(0).getStart().toByteArray(), ranges.get(ranges.size() - 1).getEnd().toByteArray(),
          new Batch.Call<QueryService, List<TrajectoryResult>>() {
            @Override
            public List<TrajectoryResult> call(QueryService queryService) throws IOException {
              BlockingRpcCallback<QueryResponse> rpcCallback = new BlockingRpcCallback();
              queryService.query(new ServerRpcController(), request, rpcCallback);
              QueryResponse response = rpcCallback.get();
              return response.getListList();
            }
          });
    } catch (Throwable e) {
      e.printStackTrace();
    }

    List<Trajectory> result = new ArrayList<>();
    for (List<TrajectoryResult> trajectoryResultList : coprocessorResult.values()) {
      for (TrajectoryResult tr : trajectoryResultList) {
        byte[] trajPointByteArray = tr.getTrajPointList().toByteArray();
        byte[] objectID = tr.getObjectId().toByteArray();
        Trajectory trajectory = TrajectorySerdeUtils.getTrajectory(trajPointByteArray, objectID);
        result.add(trajectory);
      }
    }
    return result;
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, IndexMeta> map = getAvailableIndexes();
    // find a spatial index
    if (map.containsKey(IndexType.XZ2)) {
      return map.get(IndexType.XZ2);
    } else if (map.containsKey(IndexType.XZ2T)) {
      return map.get(IndexType.XZ2);
    } else if (map.containsKey(IndexType.XZ2Plus)) {
      return map.get(IndexType.XZ2Plus);
    }
    // no spatial index so we wiil do a full table scan, we select a main index.
    for (Map.Entry<IndexType, IndexMeta> entry : map.entrySet()) {
      if (entry.getValue().isMainIndex()) {
        return entry.getValue();
      }
    }
    return null;
  }
}