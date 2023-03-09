package cn.edu.whu.trajspark.index.spatialtemporal;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.*;
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
 * row key: shard(short) + xz2(long) + xztCoding(long) + oid(max_oid_length) + tid(max_tid_length)
 *
 * @author Xu Qi
 * @since 2022/11/30
 */
public class XZ2TIndexStrategy extends IndexStrategy {

  private final XZ2Coding xz2Coding;
  private final XZTCoding xztCoding;

  public XZ2TIndexStrategy(XZ2Coding xz2Coding, XZTCoding xztCoding) {
    indexType = IndexType.XZ2T;
    this.xz2Coding = xz2Coding;
    this.xztCoding = xztCoding;
  }

  public XZ2TIndexStrategy() {
    indexType = IndexType.XZ2T;
    this.xz2Coding = new XZ2Coding();
    this.xztCoding = new XZTCoding();
  }

  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZ2Coding.BYTES + XZTCoding.BYTES_NUM + MAX_OID_LENGTH + MAX_TID_LENGTH;
  private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES;
  private static final int SCAN_RANGE_BYTE_LEN =  PHYSICAL_KEY_BYTE_LEN - MAX_OID_LENGTH - MAX_TID_LENGTH;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.index(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN);
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }

  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray xz2Bytes, ByteArray timeBytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(xz2Bytes.getBytes());
    if (end) {
      byteBuffer.putLong(Bytes.toLong(timeBytes.getBytes()) + 1);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return xztCoding.getXZTElementTimeLine(Bytes.toLong(byteArray.getBytes()));
  }

  // TODO
  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    throw new UnsupportedOperationException();
  }

  // TODO
  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    throw new UnsupportedOperationException();
  }

  // TODO
  @Override
  public List<RowKeyRange> getScanRanges(
      SpatialTemporalQueryCondition spatialTemporalQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oID) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(
      SpatialTemporalQueryCondition spatialTemporalQueryCondition) {
    List<RowKeyRange> result = new ArrayList<>();
    SpatialQueryCondition spatialQueryCondition = spatialTemporalQueryCondition.getSpatialQueryCondition();
    TemporalQueryCondition temporalQueryCondition = spatialTemporalQueryCondition.getTemporalQueryCondition();
    // 1. get xz2 coding
    List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
    for (CodingRange spatialCodingRange : spatialCodingRanges) {
      // 2. get xzt coding
      List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
      for (CodingRange timeCodingRange : temporalCodingRanges) {
        // 3. concat shard index
        for (short shard = 0; shard < shardNum; shard++) {
          ByteArray byteArray1 = toRowKeyRangeBoundary(shard, spatialCodingRange.getLower(),
              timeCodingRange.getLower(), false);
          ByteArray byteArray2 = toRowKeyRangeBoundary(shard, spatialCodingRange.getUpper(),
              timeCodingRange.getUpper(), true);
          result.add(new RowKeyRange(byteArray1, byteArray2, false));
        }
      }
    }
    return result;
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray physicalIndex) {
    return "Row key index: {" + "shardNum=" + getShardNum(physicalIndex) + ", xz2="
        + extractSpatialCode(physicalIndex) + ", bin = " + getTimeBin(physicalIndex) + ", timeCoding = "
        + getTimeElementCode(physicalIndex) + ", oidAndTid=" + getObjectID(physicalIndex) + "-" + getTrajectoryID(physicalIndex) + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    byte[] bytes = new byte[Long.BYTES];
    buffer.get(bytes);
    return new ByteArray(bytes);
  }

  @Override
  public TimeCoding getTimeCoding() {
    return xztCoding;
  }

  @Override
  public TimeBin getTimeBin(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    return xztCoding.getTimeBin(buffer.getLong());
  }

  @Override
  public long getTimeElementCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    return xztCoding.getElementCode(buffer.getLong());
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    return buffer.getShort();
  }

  @Override
  public String getObjectID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    byte[] tidBytes = new byte[MAX_TID_LENGTH];
    buffer.get(tidBytes);
    return new String(tidBytes, StandardCharsets.UTF_8);
  }

}
