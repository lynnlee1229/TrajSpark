package cn.edu.whu.trajspark.index;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.constant.IndexConstants;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * 接收对象,输出row-key.
 * 接收row-key,输出索引信息
 * 接收查询条件, 输出ROW-key范围
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public abstract class IndexStrategy implements Serializable {

  protected short shardNum = IndexConstants.DEFAULT_SHARD_NUM;

  protected IndexType indexType;

  // 对轨迹编码
  public abstract ByteArray index(Trajectory trajectory);

  public IndexType getIndexType() {
    return indexType;
  }

  public abstract TimeLine getTimeLineRange(ByteArray byteArray);

  /**
   * Get RowKey pairs for scan operation, based on spatial and temporal range.
   * A pair of RowKey is the startKey and endKey of a single scan.
   * @param spatialQueryCondition 查询框
   * @param maxRangeNum 最大范围数量
   * @return RowKey pairs
   */
  public abstract List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, int maxRangeNum);

  public abstract List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition);

  public abstract List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition spatialTemporalQueryCondition, int maxRangeNum);

  public abstract List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition, String oID);

  public abstract List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition spatialTemporalQueryCondition);

  public abstract String parseIndex2String(ByteArray byteArray);

  public abstract SpatialCoding getSpatialCoding();

  public abstract ByteArray extractSpatialCode(ByteArray byteArray);

  public abstract TimeCoding getTimeCoding();
  public abstract short getTimeBinVal(ByteArray byteArray);

  public abstract long getTimeCodingVal(ByteArray byteArray);

  public abstract short getShardNum(ByteArray byteArray);

  public abstract Object getObjectTrajId(ByteArray byteArray);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexStrategy that = (IndexStrategy) o;
    return shardNum == that.shardNum && indexType == that.indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardNum, indexType);
  }

}
