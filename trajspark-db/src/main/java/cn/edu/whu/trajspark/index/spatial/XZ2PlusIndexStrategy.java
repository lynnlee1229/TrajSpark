package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;

import java.util.List;

/**
 * row key: shard(short) + index type(int) + [xz2(long) + pos code(short)] + [oid(string) + tid(string)]
 *
 * @author Haocheng Wang
 * Created on 2022/11/1
 */
public class XZ2PlusIndexStrategy extends IndexStrategy {

  private XZ2Coding xz2Coding;

  private static final int KEY_BYTE_LEN = Short.BYTES + Integer.BYTES + Long.BYTES;



  @Override
  public ByteArray index(Trajectory trajectory) {
    return null;
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition, String oID) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition) {
    return null;
  }

  @Override
  public String indexToString(ByteArray byteArray) {
    return null;
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return null;
  }

  @Override
  public ByteArray extractSpatialCoding(ByteArray byteArray) {
    return null;
  }

  @Override
  public TimeCoding getTimeCoding() {
    return null;
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    return 0;
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    return 0;
  }

  @Override
  public Object getObjectTrajId(ByteArray byteArray) {
    return null;
  }
}
