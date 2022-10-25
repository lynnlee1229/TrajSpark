package cn.edu.whu.trajspark.index.time;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;

import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeLineCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeIndexRange;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.sfcurve.IndexRange;

/**
 * @author Xu Qi
 * @since 2022/10/7
 */
public class TimeIndexStrategy extends IndexStrategy {

  private final TimeLineCoding timeCoding;

  public TimeIndexStrategy(TimeLineCoding timeCoding) {
    this.timeCoding = timeCoding;
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeCoding.getIndex(timeLine);
    short binNum = (short) timeCoding.dateToBinnedTime(timeLine.getTimeStart()).getBin();
    return toIndex(shard, binNum, timeIndex, trajectory.getObjectID(),
        trajectory.getTrajectoryID());
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.TXZ2;
  }

  @Override
  public Polygon getSpatialRange(ByteArray byteArray) {
    return null;
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
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oId) {
    List<RowKeyRange> result = new ArrayList<>();
    // 1. xzt coding
    List<TimeIndexRange> list = timeCoding.ranges(temporalQueryCondition);
    // 2. concat shard index
    for (TimeIndexRange xztCoding : list) {
      for (short shard = 0; shard < shardNum; shard++) {
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(
            Short.BYTES + MAX_OID_LENGTH + Short.BYTES + Long.BYTES + MAX_TID_LENGTH);
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(
            Short.BYTES + MAX_OID_LENGTH + Short.BYTES + Long.BYTES + MAX_TID_LENGTH);
        byte[] oidBytes = oId.getBytes(StandardCharsets.ISO_8859_1);
        byte[] oidBytesPadding = bytePadding(oidBytes, MAX_OID_LENGTH);
        short bin = xztCoding.getTimeBin().getBin();
        byteBuffer1.putShort(shard);
        byteBuffer1.put(oidBytesPadding);
        byteBuffer1.putShort(bin);
        byteBuffer1.putLong(xztCoding.getLower());
        byteBuffer2.putShort(shard);
        byteBuffer2.put(oidBytesPadding);
        byteBuffer2.putShort(bin);
        byteBuffer2.putLong(xztCoding.getUpper());
        result.add(new RowKeyRange(new ByteArray(byteBuffer1), new ByteArray(byteBuffer2)));
      }
    }
    return result;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition) {
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
  public long getSpatialCodingVal(ByteArray byteArray) {
    return 0;
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
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    return buffer.getShort();
  }

  @Override
  public String getObjectId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    byte[] stringBytes = new byte[MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.ISO_8859_1);
  }

  @Override
  public void buildUnserializableObjects() {

  }

  public String timeIndexToString(ByteArray byteArray) {
    return "Row key index: {" + "shardNum = " + getShardNum(byteArray) + ", OID = "
        + getObjectId(byteArray) + ", Bin = " + getTimeBinVal(byteArray) + ", timeCoding = "
        + getTimeCodingVal(byteArray) + '}';
  }

  private ByteArray toIndex(short shard, short bin, long timeCode, String oId, String tId) {
    byte[] oidBytes = oId.getBytes(StandardCharsets.ISO_8859_1);
    byte[] oidBytesPadding = bytePadding(oidBytes, MAX_OID_LENGTH);
    byte[] tidBytes = tId.getBytes(StandardCharsets.ISO_8859_1);
    byte[] tidBytesPadding = bytePadding(tidBytes, MAX_TID_LENGTH);
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        Short.BYTES + MAX_OID_LENGTH + Short.BYTES + Long.BYTES + MAX_TID_LENGTH);
    byteBuffer.putShort(shard);
    byteBuffer.put(oidBytesPadding);
    byteBuffer.putShort(bin);
    byteBuffer.putLong(timeCode);
    byteBuffer.put(tidBytesPadding);
    return new ByteArray(byteBuffer);
  }

  private byte[] bytePadding(byte[] bytes, int length) {
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
