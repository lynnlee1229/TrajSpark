package cn.edu.whu.trajspark.index.time;

import static cn.edu.whu.trajspark.coding.conf.Constants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.coding.conf.Constants.MAX_TID_LENGTH;

import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeLineCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.Polygon;

/**
 * @author Xu Qi
 * @since 2022/10/7
 */
public class TimeIndexStrategy implements IndexStrategy {

  private final TimeLineCoding timeLineCoding;
  private final short shardNum;

  public TimeIndexStrategy(TimeLineCoding timeLineCoding, short shardNum) {
    this.timeLineCoding = timeLineCoding;
    this.shardNum = shardNum;
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(),
        trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeLineCoding.getIndex(timeLine);
    short binNum = (short) timeLineCoding.dateToBinnedTime(timeLine.getTimeStart()).getBin();
    return toIndex(shard, binNum, timeIndex, trajectory.getObjectID(),
        trajectory.getTrajectoryID());
  }

  @Override
  public Polygon getSpatialRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    long timeCodingVal = getTimeCodingVal(byteArray);
    long bin = getTimeBinVal(byteArray);
    TimeBin timeBin = new TimeBin(bin, timeLineCoding.getTimePeriod());
    return timeLineCoding.getTimeLine(timeCodingVal, timeBin);
  }


  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition) {
    return null;
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
    return timeLineCoding;
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
    return 0;
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
