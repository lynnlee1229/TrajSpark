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
 * row key: shard(short) + xztCoding(long) + xz2(long) +
 * oid(max_oid_length) + tid(max_tid_length)
 *
 * @author Xu Qi
 * @since 2022/11/30
 */
public class TXZ2IndexStrategy extends IndexStrategy {

  private static final long serialVersionUID = 8511586531909133379L;

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
  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZTCoding.BYTES_NUM
      + XZ2Coding.BYTES + MAX_OID_LENGTH + MAX_TID_LENGTH;
  private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES;
  private static final int SCAN_RANGE_BYTE_LEN =  PHYSICAL_KEY_BYTE_LEN - MAX_OID_LENGTH - MAX_TID_LENGTH;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.index(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN);
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }


  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray timeBytes, ByteArray xz2Bytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
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
    return xztCoding.getXZTElementTimeLine(Bytes.toLong(byteArray.getBytes()));
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
    List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
    // 四重循环，所有可能的时间编码都应单独取值
    for (CodingRange temporalCodingRange : temporalCodingRanges) {
      long lowerXZTCode = Bytes.toLong(temporalCodingRange.getLower().getBytes());
      long upperXZTCode = Bytes.toLong(temporalCodingRange.getUpper().getBytes());
      List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
      boolean tValidate = temporalCodingRange.isValidated();
      for (long xztCode = lowerXZTCode; xztCode <= upperXZTCode; xztCode++) {
        for (CodingRange spatialCodingRange : spatialCodingRanges) {
          boolean sValidate = spatialCodingRange.isValidated();
          for (short shard = 0; shard < shardNum; shard++) {
            ByteArray byteArray1 = toRowKeyRangeBoundary(shard, new ByteArray(Bytes.toBytes(xztCode)),
                spatialCodingRange.getLower(), false);
            ByteArray byteArray2 = toRowKeyRangeBoundary(shard, new ByteArray(Bytes.toBytes(xztCode)),
                spatialCodingRange.getUpper(), true);
            result.add(new RowKeyRange(byteArray1, byteArray2, tValidate && sValidate));
          }
        }
      }
    }
    return result;
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", bin = " + getTimeBin(
        byteArray) + ", timeCoding = " + getTimeElementCode(byteArray) + ", xz2="
        + extractSpatialCode(byteArray) + ", oidAndTid=" + getObjectID(byteArray) + "-" + getTrajectoryID(byteArray) + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort(); // shard
    buffer.getLong(); // time code
    byte[] bytes = new byte[XZ2Coding.BYTES];
    buffer.get(bytes);
    return new ByteArray(bytes);
  }

  @Override
  public TimeCoding getTimeCoding() {
    return xztCoding;
  }

  public long getTimeCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    return buffer.getLong();
  }

  @Override
  public TimeBin getTimeBin(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    long xztCode = buffer.getLong();
    return xztCoding.getTimeBin(xztCode);
  }

  @Override
  public long getTimeElementCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    long xztCode = buffer.getLong();
    return xztCoding.getElementCode(xztCode);
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
    byte[] stringBytes = new byte[MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
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
    byte[] tidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(tidBytes);
    return new String(tidBytes, StandardCharsets.UTF_8);
  }
}
