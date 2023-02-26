package cn.edu.whu.trajspark.index;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.constant.IndexConstants;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;

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

  /**
   * 获取轨迹在数据库中的物理索引, 即在逻辑索引之前拼接上shard, shard由逻辑索引的hash值作模运算得到。
   */
  public ByteArray index(Trajectory trajectory) {
    ByteArray logicalIndex = logicalIndex(trajectory);
    short shard = (short) Math.abs((logicalIndex.hashCode() % shardNum));
    ByteBuffer buffer = ByteBuffer.allocate(logicalIndex.getBytes().length + Short.BYTES);
    buffer.put(Bytes.toBytes(shard));
    buffer.put(logicalIndex.getBytes());
    return new ByteArray(buffer.array());
  }

  // 对轨迹编码
  protected abstract ByteArray logicalIndex(Trajectory trajectory);

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

  public abstract String parsePhysicalIndex2String(ByteArray byteArray);

  public abstract SpatialCoding getSpatialCoding();

  public abstract ByteArray extractSpatialCode(ByteArray byteArray);

  public abstract TimeCoding getTimeCoding();
  public abstract short getTimeBinVal(ByteArray byteArray);

  public abstract long getTimeCodingVal(ByteArray byteArray);

  public abstract short getShardNum(ByteArray byteArray);

  public byte[] getObjectIDBytes(Trajectory trajectory) {
    return getObjectIDBytes(trajectory.getObjectID());
  }

  public byte[] getObjectIDBytes(String oid) {
    return bytePadding(oid.getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
  }

  public byte[] getTrajectoryIDBytes(Trajectory trajectory) {
    return bytePadding(trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
  }

  private byte[] bytePadding(byte[] bytes, int length) {
    byte[] b3 = new byte[length];
    if (bytes.length < length) {
      byte[] bytes1 = new byte[length - bytes.length];
      ByteBuffer buffer = ByteBuffer.allocate(length);
      buffer.put(bytes1);
      buffer.put(bytes);
      b3 = buffer.array();
    } else {
      System.arraycopy(bytes, 0, b3, 0, length);
    }
    return b3;
  }

  public abstract String getObjectID(ByteArray byteArray);

  public abstract String getTrajectoryID(ByteArray byteArray);

  /**
   * 为避免hotspot问题, 在index中设计了salt shard.
   * 为进一步确保不同shard的数据分发至不同region server, 需要对相应的索引表作pre-split操作.
   * 本方法根据shard的数量, 生成shard-1个分割点，从而将表pre-splt为shard个region.
   * @return 本索引表的split points.
   */
  public byte[][] getSplits() {
    byte[][] splits = new byte[shardNum - 1][];
    for (int i = 0; i < shardNum - 1; i++) {
      splits[i] = Bytes.toBytes(i + 1);
    }
    return splits;
  }

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
