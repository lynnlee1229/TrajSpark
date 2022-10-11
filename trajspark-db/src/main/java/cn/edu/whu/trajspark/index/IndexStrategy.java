package cn.edu.whu.trajspark.index;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.jts.geom.Polygon;

import java.util.List;

/**
 * 接收对象,输出row-key.
 * 接收row-key,输出索引信息
 * 接收查询条件, 输出ROW-key范围
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public interface IndexStrategy {

  // 对轨迹编码
  ByteArray index(Trajectory trajectory) throws Exception;

  Polygon getSpatialRange(ByteArray byteArray);
  TimeLine getTimeLineRange(ByteArray byteArray);

  /**
   * Get RowKey pairs for scan operation, based on spatial and temporal range.
   * A pair of RowKey is the startKey and endKey of a single scan.
   * @param spatialQueryCondition 查询框
   * @param maxRangeNum 最大范围数量
   * @return RowKey pairs
   */
  List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, int maxRangeNum);

  List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition);

  List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition, int maxRangeNum);

  List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition);

  List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition);

  String indexToString(ByteArray byteArray);

  SpatialCoding getSpatialCoding();

  long getSpatialCodingVal(ByteArray byteArray);

  TimeCoding getTimeCoding();

  long getTimeCodingVal(ByteArray byteArray);

  short getShardNum(ByteArray byteArray);

  String getTrajectoryId(ByteArray byteArray);


}
