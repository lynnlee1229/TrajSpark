package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.geomesa.curve.XZ2SFC;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.sfcurve.IndexRange;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * row key: shard(short) + index type(int) + xz2(long) + oid(string) + tid(string)
 *
 * @author Haocheng Wang Created on 2022/10/4
 */
public class XZ2IndexStrategy extends IndexStrategy {

  private XZ2Coding xz2Coding;
  private static final int KEY_BYTE_LEN = Short.BYTES + Integer.BYTES + Long.BYTES;

  public XZ2IndexStrategy() {
    indexType = IndexType.XZ2;
    this.xz2Coding = new XZ2Coding();
  }

  public XZ2IndexStrategy(int maxPrecision) {
    this();
    this.xz2Coding = new XZ2Coding((short) maxPrecision);
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    long spatialCodingVal = xz2Coding.index(trajectory.getLineString().getEnvelopeInternal());
    String oid = trajectory.getObjectID();
    String tid = trajectory.getTrajectoryID();
    return toIndex(shard, spatialCodingVal, oid + tid);
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.XZ2;
  }

  @Override
  public Polygon getSpatialRange(ByteArray byteArray) {
    long spatialCodingVal = getSpatialCodingVal(byteArray);
    return xz2Coding.getSpatialPolygon(spatialCodingVal);
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
    List<IndexRange> list = xz2Coding.ranges(spatialQueryCondition);
    // 2. concat shard index
    for (IndexRange xz2Coding : list) {
      for (short shard = 0; shard < shardNum; shard++) {
        // make sure range end is exclusive
        result.add(new RowKeyRange(toIndex(shard, xz2Coding.lower())
            , toIndex(shard, xz2Coding.upper() + 1L)));
      }
    }
    return result;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition) {
    return getScanRanges(spatialQueryCondition, 500);
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition,
      String oID) {
    throw new UnsupportedOperationException();
  }


  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String indexToString(ByteArray byteArray) {
    return "Row key index: {" +
        "shardNum=" + getShardNum(byteArray) +
        ", xz2=" + getSpatialCodingVal(byteArray) +
        ", tid=" + getObjectId(byteArray) +
        '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
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
  public String getObjectId(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getLong();
    byte[] stringBytes = new byte[buffer.capacity() - KEY_BYTE_LEN];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  @Override
  public void buildUnserializableObjects() {
    xz2Coding.setXz2Sfc(XZ2SFC.apply((short) xz2Coding.getXz2Precision()));
  }

  private ByteArray toIndex(short shard, long xz2coding, String oidAndTid) {
    byte[] oidAndTidBytes = oidAndTid.getBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN + oidAndTidBytes.length);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.putLong(xz2coding);
    byteBuffer.put(oidAndTidBytes);
    return new ByteArray(byteBuffer);
  }

  private ByteArray toIndex(short shard, long xz2coding) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(KEY_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.putInt(indexType.getId());
    byteBuffer.putLong(xz2coding);
    return new ByteArray(byteBuffer);
  }

  @Override
  public String toString() {
    return "XZ2IndexStrategy{" +
        "shardNum=" + shardNum +
        ", indexType=" + indexType +
        ", xz2Coding=" + xz2Coding +
        '}';
  }
}
