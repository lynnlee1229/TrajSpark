package cn.edu.whu.trajspark.index.time;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.CodingRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;

/**
 * row key: shard(short) + oid(string) + XZTCoding(long) + tid(string)
 *
 * @author Xu Qi
 * @since 2022/10/7
 */
public class IDTIndexStrategy extends IndexStrategy {

  private final XZTCoding timeCoding;

  /**
   * 作为行键时的字节数
   */
  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + MAX_OID_LENGTH + XZTCoding.BYTES_NUM + MAX_TID_LENGTH;
  private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES;
  private static final int SCAN_RANGE_BYTE_LEN =  PHYSICAL_KEY_BYTE_LEN - MAX_TID_LENGTH;

  public IDTIndexStrategy(XZTCoding timeCoding) {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = timeCoding;
  }
  public IDTIndexStrategy() {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = new XZTCoding();
  }

  @Override
  /**
   * ID-T索引中，shard由object id的hashcode生成，在负载均衡的同时，同ID数据保持本地性
   */
  public ByteArray index(Trajectory trajectory) {
    ByteArray logicalIndex = logicalIndex(trajectory);
    short shard = getShard(trajectory.getObjectID());
    ByteBuffer buffer = ByteBuffer.allocate(logicalIndex.getBytes().length + Short.BYTES);
    buffer.put(Bytes.toBytes(shard));
    buffer.put(logicalIndex.getBytes());
    return new ByteArray(buffer.array());
  }

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeCoding.getIndex(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN);
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.putLong(timeIndex);
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }

  /**
   * 先去掉头部的shard和OID信息
   */
  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return timeCoding.getXZTElementTimeLine(getTimeCode(byteArray));
  }


  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(
      SpatialTemporalQueryCondition spatialTemporalQueryCondition, int maxRangeNum) {
    return null;
  }

  /**
   * @param temporalQueryCondition Time query range
   * @param oId                    Trajectory ID
   * @return List of XZT index ranges corresponding to the query range.
   */
  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oId) {
    List<RowKeyRange> result = new ArrayList<>();
    List<CodingRange> codingRanges = timeCoding.ranges(temporalQueryCondition);
    for (CodingRange codingRange : codingRanges) {
      short shard = getShard(oId);
      ByteArray byteArray1 = toRowKeyRangeBoundary(shard, codingRange.getLower(), oId, false);
      ByteArray byteArray2 = toRowKeyRangeBoundary(shard, codingRange.getUpper(), oId, true);
      result.add(new RowKeyRange(byteArray1, byteArray2, codingRange.isContained()));

    }
    return result;
  }

  @Override
  public List<RowKeyRange> getScanRanges(
      SpatialTemporalQueryCondition spatialTemporalQueryCondition) {
    return null;
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {" + "shardNum = " + getShardNum(byteArray) + ", OID = " + getObjectId(
        byteArray) + ", XZT = " + timeCoding.getXZTElementTimeLine(getTimeCode(byteArray)) + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return null;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    return null;
  }


  @Override
  public TimeCoding getTimeCoding() {
    return timeCoding;
  }

  @Override
  public long getTimeElementCode(ByteArray byteArray) {
    return timeCoding.getElementCode(getTimeCode(byteArray));
  }

  public TimeBin getTimeBin(ByteArray byteArray) {
    return timeCoding.getTimeBin(getTimeCode(byteArray));
  }

  public long getTimeCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    for (int i = 0; i < MAX_OID_LENGTH; i++) {
      buffer.get();
    }
    return buffer.getLong();
  }

  @Override
  public short getShardNum(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    return buffer.getShort();
  }

  @Override
  public String getObjectID(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort(); // shard
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    // shard
    buffer.getShort();
    // OID
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    // time code
    buffer.getLong();
    // TID
    byte[] tidBytes = new byte[MAX_TID_LENGTH];
    buffer.get(tidBytes);
    return new String(tidBytes, StandardCharsets.UTF_8);
  }


  public String getObjectId(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    byte[] stringBytes = new byte[MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray timeBytes, String oId, Boolean end) {
    byte[] oidBytesPadding = getObjectIDBytes(oId);
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(oidBytesPadding);
    if (end) {
      byteBuffer.putLong(Bytes.toLong(timeBytes.getBytes()) + 1);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  private short getShard(String oid) {
    return (short) Math.abs(oid.hashCode() % shardNum);
  }
}
