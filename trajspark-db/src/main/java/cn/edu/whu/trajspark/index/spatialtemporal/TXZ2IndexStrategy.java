package cn.edu.whu.trajspark.index.spatialtemporal;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;

import cn.edu.whu.trajspark.coding.CodingRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * row key: shard(short) + index type(int) + xztCoding(short + long) + xz2(long) +
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

  // range key: shard(short) + index type(int) + xzt(long) + xz2(long)
  private static final int KEY_BYTE_LEN =
      Short.BYTES + Integer.BYTES + XZTCoding.BYTES + XZ2Coding.BYTES;

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.code(timeLine);
    String oid = trajectory.getObjectID();
    String tid = trajectory.getTrajectoryID();
    return toIndex(shard, timeCode, spatialCoding, oid + tid);
  }

  private ByteArray toIndex(short shard, ByteArray timeCoding, ByteArray xz2coding,
      String oidAndTid) {
    byte[] oidAndTidBytes = oidAndTid.getBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN + oidAndTidBytes.length);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.put(timeCoding.getBytes());
    byteBuffer.put(xz2coding.getBytes());
    byteBuffer.put(oidAndTidBytes);
    return new ByteArray(byteBuffer);
  }

  private ByteArray toIndex(short shard, ByteArray timeBytes, ByteArray xz2Bytes, Boolean flag) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.put(timeBytes.getBytes());
    if (flag) {
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
    List<RowKeyRange> result = new ArrayList<>();
    SpatialQueryCondition spatialQueryCondition = spatialTemporalQueryCondition.getSpatialQueryCondition();
    TemporalQueryCondition temporalQueryCondition = spatialTemporalQueryCondition.getTemporalQueryCondition();
    // 1. get xzt coding
    List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
    for (CodingRange temporalCodingRange : temporalCodingRanges) {
      // 2. get xz2 coding
      List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
      // 3. concat shard index
      for (CodingRange codingRange : spatialCodingRanges) {
        for (short shard = 0; shard < shardNum; shard++) {
          ByteBuffer byteBuffer1 = ByteBuffer.allocate(KEY_BYTE_LEN);
          ByteBuffer byteBuffer2 = ByteBuffer.allocate(KEY_BYTE_LEN);
          ByteArray byteArray1 = toIndex(shard, temporalCodingRange.getLower(),
              codingRange.getLower(), false);
          ByteArray byteArray2 = toIndex(shard, temporalCodingRange.getUpper(),
              codingRange.getUpper(), true);
          byteBuffer1.put(byteArray1.getBytes());
          byteBuffer2.put(byteArray2.getBytes());
          result.add(new RowKeyRange(new ByteArray(byteBuffer1), new ByteArray(byteBuffer2),
              codingRange.isContained() & temporalCodingRange.isContained()));
        }
      }
    }
    return result;
  }

  @Override
  public String parseIndex2String(ByteArray byteArray) {
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
    buffer.getShort();
    buffer.getInt();
    buffer.getShort();
    buffer.getLong();
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
    buffer.getInt();
    return buffer.getShort();
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getInt();
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
    buffer.getInt();
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }
}
