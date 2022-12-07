package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.coding.*;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.IndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * row key: shard(short) + index type(int) + XZPCode + [oid(string) + tid(string)]
 *
 * @author Haocheng Wang Created on 2022/11/1
 */
public class XZ2PlusIndexStrategy extends IndexStrategy {

  private XZ2PCoding xz2PCoding;

  private static final int KEY_BYTE_LEN = Short.BYTES + Integer.BYTES + XZ2PCoding.BYTES;

  public XZ2PlusIndexStrategy() {
    indexType = IndexType.XZ2Plus;
    this.xz2PCoding = new XZ2PCoding();
  }

  @Override
  public ByteArray index(Trajectory trajectory) {
    List<byte[]> elements = new LinkedList<>();
    // 1. shard
    elements.add(Bytes.toBytes((short) (Math.random() * shardNum)));
    // 2. index type
    elements.add(Bytes.toBytes(getIndexType().getId()));
    // 3. xz2p code
    elements.add(xz2PCoding.code(trajectory.getLineString()).getBytes());
    // 4. oid
    elements.add(trajectory.getObjectID().getBytes());
    // 5. tid
    elements.add(trajectory.getTrajectoryID().getBytes());
    return new ByteArray(elements);
  }

  @Override
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return null;
  }

  @Override
  public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
      int maxRangeNum) {
    List<RowKeyRange> result = new ArrayList<>();
    // 1. xz2p coding
    List<CodingRange> codeRanges = xz2PCoding.ranges(spatialQueryCondition);
    List<CodingRange> keyScanRange = getKeyScanRange(codeRanges);

    // 2. concat shard index and index type.
    for (CodingRange xz2PCode : keyScanRange) {
      for (short shard = 0; shard < shardNum; shard++) {
        result.add(new RowKeyRange(new ByteArray(
            Arrays.asList(Bytes.toBytes(shard), Bytes.toBytes(indexType.getId()),
                xz2PCode.getLower().getBytes())), new ByteArray(
            Arrays.asList(Bytes.toBytes(shard), Bytes.toBytes(indexType.getId()),
                xz2PCode.getUpper().getBytes())), xz2PCode.isContained()));
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
    return null;
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
    return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", indexId=" + getIndexType()
        + ", xz2P=" + extractSpatialCode(byteArray) + ", oidTid=" + getObjectTrajId(byteArray)
        + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2PCoding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getInt();
    byte[] codingByteArray = new byte[XZ2PCoding.BYTES];
    buffer.get(codingByteArray);
    return new ByteArray(codingByteArray);
  }

  @Override
  public TimeCoding getTimeCoding() {
    return null;
  }

  @Override
  public short getTimeBinVal(ByteArray byteArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimeCodingVal(ByteArray byteArray) {
    return 0;
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    return buffer.getShort();
  }

  @Override
  public Object getObjectTrajId(ByteArray byteArray) {
    int allLen = byteArray.getBytes().length;
    ByteBuffer buffer = byteArray.toByteBuffer();
    buffer.flip();
    buffer.getShort();
    buffer.getInt();
    byte[] codingByteArray = new byte[XZ2PCoding.BYTES];
    buffer.get(codingByteArray);
    int objTIDArrayLen = allLen - Short.BYTES - Long.BYTES - XZ2PCoding.BYTES;
    byte[] objTIDArray = new byte[objTIDArrayLen];
    buffer.get(objTIDArray);
    return new String(objTIDArray, StandardCharsets.UTF_8);
  }

  private List<CodingRange> getKeyScanRange(List<CodingRange> codingRanges) {
    List<CodingRange> codingRangesList = new ArrayList<>();
    for (CodingRange codingRange : codingRanges) {
      ByteArray lower = codingRange.getLower();
      ByteArray upper = codingRange.getUpper();
      if (codingRange.isContained()) {
        ByteBuffer byteBufferTemp = ByteBuffer.allocate(Long.BYTES);
        ByteBuffer byteBuffer = upper.toByteBuffer();
        byteBuffer.flip();
        long xz2Coding = byteBuffer.getLong() + 1;
        byteBufferTemp.putLong(xz2Coding);
        ByteArray newUpper = new ByteArray(byteBufferTemp);
        codingRangesList.add(new CodingRange(lower, newUpper, codingRange.isContained()));
      } else {
        ByteBuffer byteBufferTemp = ByteBuffer.allocate(Long.BYTES + Byte.BYTES);
        ByteBuffer byteBuffer = upper.toByteBuffer();
        byteBuffer.flip();
        long xz2Coding = byteBuffer.getLong();
        byte posCode = (byte) (byteBuffer.get() + 1);
        byteBufferTemp.putLong(xz2Coding);
        byteBufferTemp.put(posCode);
        ByteArray newUpper = new ByteArray(byteBufferTemp);
        codingRangesList.add(new CodingRange(lower, newUpper, codingRange.isContained()));
      }
    }
    return codingRangesList;
  }
}
