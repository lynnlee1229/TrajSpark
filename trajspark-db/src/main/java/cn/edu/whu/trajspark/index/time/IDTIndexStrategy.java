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
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;

/**
 * row key: shard(short) + oid(string) + XZTCoding(short + long) + tid(string)
 *
 * @author Xu Qi
 * @since 2022/10/7
 */
public class IDTIndexStrategy extends IndexStrategy {

  private final XZTCoding timeCoding;

  /**
   * 作为行键时的字节数
   */
  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + MAX_OID_LENGTH + XZTCoding.BYTES + MAX_TID_LENGTH;

  public IDTIndexStrategy(XZTCoding timeCoding) {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = timeCoding;
  }
  public IDTIndexStrategy() {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = new XZTCoding();
  }

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeCoding.getIndex(timeLine);
    short binNum = timeCoding.dateToBinnedTime(timeLine.getTimeStart()).getBin();
    byte[] oidBytesPadded = bytePadding(trajectory.getObjectID().getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
    byte[] tidBytesPadded = bytePadding(trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8), MAX_TID_LENGTH);
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN - Short.BYTES);
    byteBuffer.put(oidBytesPadded);
    byteBuffer.putShort(binNum);
    byteBuffer.putLong(timeIndex);
    byteBuffer.put(tidBytesPadded);
    return new ByteArray(byteBuffer);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    long timeCodingVal = getTimeCodingVal(byteArray);
    short bin = getTimeBinVal(byteArray);
    TimeBin timeBin = new TimeBin(bin, timeCoding.getTimePeriod());
    return timeCoding.getTimeLine(timeCodingVal, timeBin);
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
      for (short shard = 0; shard < shardNum; shard++) {
        ByteArray byteArray1 = toRowKeyRangeBoundary(shard, codingRange.getLower(), oId, false);
        ByteArray byteArray2 = toRowKeyRangeBoundary(shard, codingRange.getUpper(), oId, true);
        result.add(new RowKeyRange(byteArray1, byteArray2, codingRange.isContained()));
      }
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
        byteArray) + ", Bin = " + getTimeBinVal(byteArray) + ", timeCoding = " + getTimeCodingVal(
        byteArray) + '}';
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
  public long getTimeCodingVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    for (int i = 0; i < MAX_OID_LENGTH; i++) {
      buffer.get();
    }
    buffer.getShort();
    return buffer.getLong();
  }

  public short getTimeBinVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    for (int i = 0; i < MAX_OID_LENGTH; i++) {
      buffer.get();
    }
    return buffer.getShort();
  }

  @Override
  public short getShardNum(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    buffer.flip();
    return buffer.getShort();
  }

  @Override
  public Object getObjectTrajId(ByteArray byteArray) {
    return null;
  }

  public String getObjectId(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    byte[] stringBytes = new byte[MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  private ByteArray toRowKeyRangeBoundary(short shard, ByteArray timeBytes, String oId, Boolean end) {
    byte[] oidBytesPadding = bytePadding(oId.getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
    ByteBuffer byteBuffer = ByteBuffer.allocate(PHYSICAL_KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(oidBytesPadding);
    if (end) {
      Tuple2<Short, Long> extractTimeKeyBytes = XZTCoding.getExtractTimeKeyBytes(timeBytes);
      byteBuffer.putShort(extractTimeKeyBytes._1);
      byteBuffer.putLong(extractTimeKeyBytes._2);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  public static byte[] bytePadding(byte[] bytes, int length) {
    byte[] b3 = new byte[length];
    if (bytes.length < length) {
      byte[] bytes1 = new byte[length - bytes.length];
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write(bytes1, 0, bytes1.length);
      bos.write(bytes, 0, bytes.length);
      b3 = bos.toByteArray();
    }
    return b3;
  }

}
