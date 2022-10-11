package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZ2PlusCoding;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.sfcurve.IndexRange;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * row key: shard + xz2 + tid, 无PosCode
 * TODO: 加PosCode
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class SpatialIndexStrategy implements IndexStrategy {

  private SpatialCoding spatialCoding;
  private short shardNum;
  private int KEY_BYTE_LEN = Short.BYTES + Long.BYTES;

  public SpatialIndexStrategy(SpatialCoding spatialCoding, short shardNum) {
    this.spatialCoding = spatialCoding;
    this.shardNum = shardNum;
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    long spatialCodingVal = spatialCoding.index(trajectory.getLineString().getEnvelopeInternal());
    String tId = trajectory.getObjectID();
    return toIndex(shard, spatialCodingVal, tId);
  }

  @Override
  public Polygon getSpatialRange(ByteArray byteArray) {
    long spatialCodingVal = getSpatialCodingVal(byteArray);
    return spatialCoding.getSpatialPolygon(spatialCodingVal);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, int maxRangeNum) {
    List<RowKeyRange> result = new ArrayList<>();
    // 1. xz2 coding
    List<IndexRange> list = spatialCoding.ranges(spatialQueryCondition);
    // 2. concat shard index
    for (IndexRange xz2Coding : list) {
      for (short shard = 0; shard < shardNum; shard++) {
        ByteBuffer bb1 = ByteBuffer.allocate(KEY_BYTE_LEN);
        ByteBuffer bb2 = ByteBuffer.allocate(KEY_BYTE_LEN);
        bb1.putShort(shard);
        bb1.putLong(xz2Coding.lower());
        bb2.putShort(shard);
        bb2.putLong(xz2Coding.upper());
        result.add(new RowKeyRange(new ByteArray(bb1), new ByteArray(bb2)));
      }
    }
    return result;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    return getScanRanges(spatialQueryCondition, 500);
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String indexToString(ByteArray byteArray) {
    return "Row key index: {" +
        "shardNum=" + getShardNum(byteArray) +
        ", xz2=" + getSpatialCodingVal(byteArray) +
        ", tid=" + getTrajectoryId(byteArray) +
        '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return spatialCoding;
  }

  @Override
  public long getSpatialCodingVal(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    return buffer.getLong();
  }

  @Override
  public TimeCoding getTimeCoding() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    return buffer.getShort();
  }

  @Override
  public String getTrajectoryId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.ISO_8859_1);
  }

  private ByteArray toIndex(short shard, long xz2coding, String tId) {
    byte[] idBytes = tId.getBytes(StandardCharsets.ISO_8859_1);
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN + idBytes.length);
    byteBuffer.putShort(shard);
    byteBuffer.putLong(xz2coding);
    for (byte idByte : idBytes) {
      byteBuffer.put(idByte);
    }
    return new ByteArray(byteBuffer);
  }
}
