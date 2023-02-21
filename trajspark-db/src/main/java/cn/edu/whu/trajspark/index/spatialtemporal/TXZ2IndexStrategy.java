package cn.edu.whu.trajspark.index.spatialtemporal;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.*;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;

/**
 * row key: shard(short) + xztCoding(short + long) + xz2(long) +
 * oidAndTid(string)
 *
 * @author Xu Qi
 * @since 2022/11/30
 */
public class TXZ2IndexStrategy extends IndexStrategy {

  private final XZ2Coding xz2Coding;
  private final XZTCoding xztCoding;

  public TXZ2IndexStrategy(XZTCoding xztCoding, XZ2Coding xz2Coding) {
    indexType = IndexType.TXZ2;
    this.xztCoding = xztCoding;
    this.xz2Coding = xz2Coding;
  }
  public TXZ2IndexStrategy() {
    indexType = IndexType.TXZ2;
    this.xztCoding = new XZTCoding();
    this.xz2Coding = new XZ2Coding();
  }

  /**
   * 作为行键时的字节数
   */
  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZTCoding.BYTES
      + XZ2Coding.BYTES + MAX_OID_LENGTH + MAX_TID_LENGTH;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.code(timeLine);
    String oid = trajectory.getObjectID();
    String tid = trajectory.getTrajectoryID();
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN - Short.BYTES);
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(IDTIndexStrategy.bytePadding(oid.getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH));
    byteBuffer.put(IDTIndexStrategy.bytePadding(tid.getBytes(StandardCharsets.UTF_8), MAX_TID_LENGTH));
    return new ByteArray(byteBuffer);
  }


  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray timeBytes, ByteArray xz2Bytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN - MAX_OID_LENGTH - MAX_TID_LENGTH);
    byteBuffer.putShort(shard);
    byteBuffer.put(timeBytes.getBytes());
    if (end) {
      long xz2code = Bytes.toLong(xz2Bytes.getBytes()) + 1;
      byteBuffer.putLong(xz2code);
    } else {
      byteBuffer.put(xz2Bytes.getBytes());
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

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    throw new UnsupportedOperationException();
  }

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
    if (spatialTemporalQueryCondition.getTemporalQueryCondition() == null) {
      throw new UnsupportedOperationException();
    }
    List<RowKeyRange> result = new ArrayList<>();
    SpatialQueryCondition spatialQueryCondition = spatialTemporalQueryCondition.getSpatialQueryCondition();
    TemporalQueryCondition temporalQueryCondition = spatialTemporalQueryCondition.getTemporalQueryCondition();
    // 1. get xzt coding
    List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
    for (CodingRange temporalCodingRange : temporalCodingRanges) {
      // 2. get xz2 coding
      List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
      for (CodingRange codingRange : spatialCodingRanges) {
        // 3. concat shard index
        for (short shard = 0; shard < shardNum; shard++) {
          ByteArray byteArray1 = toRowKeyRangeBoundary(shard, temporalCodingRange.getLower(),
              codingRange.getLower(), false);
          ByteArray byteArray2 = toRowKeyRangeBoundary(shard, temporalCodingRange.getUpper(),
              codingRange.getUpper(), true);
          result.add(new RowKeyRange(byteArray1, byteArray2, false));
        }
      }
    }
    return result;
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", bin = " + getTimeBinVal(
        byteArray) + ", timeCoding = " + getTimeCodingVal(byteArray) + ", xz2="
        + extractSpatialCode(byteArray) + ", oidAndTid=" + getObjectTrajId(byteArray) + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort(); // shard
    buffer.getShort(); // time bin
    buffer.getLong(); // time code
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
    return buffer.getShort();
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
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
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - PHYSICAL_KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }
}
