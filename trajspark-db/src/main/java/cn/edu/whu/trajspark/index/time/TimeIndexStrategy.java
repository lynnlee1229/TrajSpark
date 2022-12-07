package cn.edu.whu.trajspark.index.time;

import static cn.edu.whu.trajspark.constant.CodingConstants.DEFAULT_TIME_PERIOD;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TIME_BIN_PRECISION;

import cn.edu.whu.trajspark.coding.CodingRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.sfc.XZTSFC;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.coding.sfc.TimeIndexRange;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

/**
 * row key: shard(short) + index type(int) + oid(string) + XZTCoding(short + long) + tid(string)
 *
 * @author Xu Qi
 * @since 2022/10/7
 */
public class TimeIndexStrategy extends IndexStrategy {

  private final XZTCoding timeCoding;
  private static final int KEY_BYTE_LEN =
      Short.BYTES + Integer.BYTES + MAX_OID_LENGTH + XZTCoding.BYTES;

  public TimeIndexStrategy(XZTCoding timeCoding) {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = timeCoding;
  }
  public TimeIndexStrategy() {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = new XZTCoding();
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
    // 1. xzt coding
    List<CodingRange> codingRanges = timeCoding.ranges(temporalQueryCondition);
    // 2. concat shard index
    for (CodingRange codingRange : codingRanges) {
      for (short shard = 0; shard < shardNum; shard++) {
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(KEY_BYTE_LEN);
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(KEY_BYTE_LEN);
        ByteArray byteArray1 = toIndex(shard, codingRange.getLower(), oId, false);
        ByteArray byteArray2 = toIndex(shard, codingRange.getUpper(), oId, true);
        byteBuffer1.put(byteArray1.getBytes());
        byteBuffer2.put(byteArray2.getBytes());
        result.add(new RowKeyRange(new ByteArray(byteBuffer1), new ByteArray(byteBuffer2),
            codingRange.isContained()));
      }
    }
    return result;
  }

  public List<RowKeyRange> getMergeScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oId) {
    List<RowKeyRange> result = new ArrayList<>();
    // 1. xzt coding
    List<CodingRange> codingRanges = timeCoding.rangesMerged(temporalQueryCondition);
    // 2. concat shard index
    for (CodingRange codingRange : codingRanges) {
      for (short shard = 0; shard < shardNum; shard++) {
        ByteBuffer byteBuffer1 = ByteBuffer.allocate(KEY_BYTE_LEN);
        ByteBuffer byteBuffer2 = ByteBuffer.allocate(KEY_BYTE_LEN);
        ByteArray byteArray1 = toIndex(shard, codingRange.getLower(), oId, false);
        ByteArray byteArray2 = toIndex(shard, codingRange.getUpper(), oId, true);
        byteBuffer1.put(byteArray1.getBytes());
        byteBuffer2.put(byteArray2.getBytes());
        result.add(new RowKeyRange(new ByteArray(byteBuffer1), new ByteArray(byteBuffer2),
            codingRange.isContained()));
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
  public String parseIndex2String(ByteArray byteArray) {
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
    buffer.getInt();
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
    buffer.getInt();
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
  public Object getObjectTrajId(ByteArray byteArray) {
    return null;
  }

  public String getObjectId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getInt();
    byte[] stringBytes = new byte[MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.ISO_8859_1);
  }

  private ByteArray toIndex(short shard, short bin, long timeCode, String oId, String tId) {
    byte[] oidBytes = oId.getBytes(StandardCharsets.ISO_8859_1);
    byte[] oidBytesPadding = bytePadding(oidBytes, MAX_OID_LENGTH);
    byte[] tidBytes = tId.getBytes(StandardCharsets.ISO_8859_1);
    byte[] tidBytesPadding = bytePadding(tidBytes, MAX_TID_LENGTH);
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        Short.BYTES + Integer.BYTES + MAX_OID_LENGTH + Short.BYTES + Long.BYTES + MAX_TID_LENGTH);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.put(oidBytesPadding);
    byteBuffer.putShort(bin);
    byteBuffer.putLong(timeCode);
    byteBuffer.put(tidBytesPadding);
    return new ByteArray(byteBuffer);
  }

  private ByteArray toIndex(short shard, ByteArray timeBytes, String oId, Boolean flag) {
    byte[] oidBytes = oId.getBytes(StandardCharsets.ISO_8859_1);
    byte[] oidBytesPadding = bytePadding(oidBytes, MAX_OID_LENGTH);
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.put(oidBytesPadding);
    if (flag) {
      Tuple2<Short, Long> extractTimeKeyBytes = timeCoding.getExtractTimeKeyBytes(timeBytes);
      byteBuffer.putShort(extractTimeKeyBytes._1);
      byteBuffer.putLong(extractTimeKeyBytes._2);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
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
