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
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * row key: shard(short) + xz2(long) + xztCoding(short + long) + oidAndTid(string)
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

  /**
   * 作为行键时的字节数，不包含oid与tid。
   */
  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZ2Coding.BYTES + XZTCoding.BYTES;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.code(timeLine);
    String oid = trajectory.getObjectID();
    String tid = trajectory.getTrajectoryID();
    byte[] oidAndTidBytes = (oid + tid).getBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN + oidAndTidBytes.length - Short.BYTES);
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(oidAndTidBytes);
    return new ByteArray(byteBuffer);
  }

  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray xz2Bytes, ByteArray timeBytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(xz2Bytes.getBytes());
    if (end) {
      Tuple2<Short, Long> extractTimeKeyBytes = XZTCoding.getExtractTimeKeyBytes(timeBytes);
      byteBuffer.putShort(extractTimeKeyBytes._1);
      byteBuffer.putLong(extractTimeKeyBytes._2);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    long timeCodingVal = getTimeCodingVal(byteArray);
    short bin = getTimeBinVal(byteArray);
    TimeBin timeBin = new TimeBin(bin, xztCoding.getTimePeriod());
    return xztCoding.getTimeLine(timeCodingVal, timeBin);
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
        + extractSpatialCode(physicalIndex) + ", bin = " + getTimeBinVal(physicalIndex) + ", timeCoding = "
        + getTimeCodingVal(physicalIndex) + ", oidAndTid=" + getObjectTrajId(physicalIndex) + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
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
  public short getTimeBinVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    return buffer.getShort();
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getShort();
    return buffer.getLong();
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    return buffer.getShort();
  }

  @Override
  public Object getObjectTrajId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getShort();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - PHYSICAL_KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }
}
