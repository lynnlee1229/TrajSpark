package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.CodingRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * row key: shard(short) + xz2(long) + oid(string) + tid(string)
 *
 * @author Haocheng Wang Created on 2022/10/4
 */
public class XZ2IndexStrategy extends IndexStrategy {

  private XZ2Coding xz2Coding;

  // shard(short) + xz2(long)
  private static final int KEY_BYTE_LEN = Short.BYTES + XZ2Coding.BYTES;

  public XZ2IndexStrategy() {
    indexType = IndexType.XZ2;
    this.xz2Coding = new XZ2Coding();
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    String oid = trajectory.getObjectID();
    String tid = trajectory.getTrajectoryID();
    return toIndex(shard, spatialCoding, oid + tid);
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.XZ2;
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    List<RowKeyRange> result = new ArrayList<>();
    // 1. xz2 coding
    List<CodingRange> codingRanges = xz2Coding.ranges(spatialQueryCondition);
    // 2. concat shard index
    for (CodingRange xz2Coding : codingRanges) {
      for (short shard = 0; shard < shardNum; shard++) {
        result.add(new RowKeyRange(toIndex(shard, xz2Coding.getLower(), false),
            toIndex(shard, xz2Coding.getUpper(), true), xz2Coding.isContained()));
      }
    }
    return result;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    return getScanRanges(spatialQueryCondition, 500);
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition spatialTemporalQueryCondition, int maxRangeNum) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oID) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition spatialTemporalQueryCondition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String parseIndex2String(ByteArray byteArray) {
    return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", xz2="
        + extractSpatialCode(byteArray) + ", tid=" + getObjectTrajId(byteArray) + '}';
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
    byte[] bytes = new byte[XZ2Coding.BYTES];
    buffer.get(bytes);
    return new ByteArray(bytes);
  }

  @Override
  public TimeCoding getTimeCoding() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getTimeBinVal(ByteArray byteArray) {
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
  public String getObjectTrajId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  private ByteArray toIndex(short shard, ByteArray xz2coding, String oidAndTid) {
    byte[] oidAndTidBytes = oidAndTid.getBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN + oidAndTidBytes.length);
    byteBuffer.putShort(shard);
    byteBuffer.put(xz2coding.getBytes());
    byteBuffer.put(oidAndTidBytes);
    return new ByteArray(byteBuffer);
  }

  private ByteArray toIndex(short shard, ByteArray xz2coding, Boolean isEndCoding) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    if (isEndCoding) {
      long xz2code = Bytes.toLong(xz2coding.getBytes()) + 1;
      byteBuffer.putLong(xz2code);
    } else {
      byteBuffer.put(xz2coding.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  @Override
  public String toString() {
    return "XZ2IndexStrategy{" + "shardNum=" + shardNum + ", indexType=" + indexType
        + ", xz2Coding=" + xz2Coding + '}';
  }
}
