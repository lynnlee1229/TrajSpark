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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class SpatialIndexStrategy implements IndexStrategy {

  private XZ2PlusCoding xz2PlusCoding;
  private short shardNum;

  public SpatialIndexStrategy(XZ2PlusCoding xz2PlusCoding, short shardNum) {
    this.xz2PlusCoding = xz2PlusCoding;
    this.shardNum = shardNum;
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    short shard = (short) (Math.random() * shardNum);
    long spatialCoding = xz2PlusCoding.index(trajectory.getLineString().getEnvelopeInternal());
    String tId = trajectory.getObjectID();
    return toIndex(shard, spatialCoding, tId);
  }

  @Override
  public Polygon getSpatialRange(ByteArray byteArray) {
    long spatialCodingVal = getSpatialCodingVal(byteArray);
    return xz2PlusCoding.getSpatialPolygon(spatialCodingVal);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition, int maxRangeNum) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(TemporalQueryCondition temporalQueryCondition) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition, TemporalQueryCondition temporalQueryCondition) {
    return null;
  }

  @Override
  public String indexToString(ByteArray byteArray) {
    return null;
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2PlusCoding;
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
    return buffer.getShort();
  }


  private ByteArray toIndex(short shard, long xz2coding, String tId) {
    byte[] idBytes = tId.getBytes(StandardCharsets.ISO_8859_1);
    ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES + Long.BYTES + idBytes.length);
    byteBuffer.putShort(shard);
    byteBuffer.putLong(xz2coding);
    for (byte idByte : idBytes) {
      byteBuffer.put(idByte);
    }
    return new ByteArray(byteBuffer);
  }
}
