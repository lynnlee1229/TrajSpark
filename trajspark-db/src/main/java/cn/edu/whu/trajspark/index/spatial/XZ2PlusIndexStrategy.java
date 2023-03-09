package cn.edu.whu.trajspark.index.spatial;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.CodingRange;
import cn.edu.whu.trajspark.coding.SpatialCoding;
import cn.edu.whu.trajspark.coding.TimeCoding;
import cn.edu.whu.trajspark.coding.XZ2PCoding;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_OID_LENGTH;
import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TID_LENGTH;

/**
 * row key: shard(short) + XZPCode + oid(max_oid_length) + tid(max_tid_length)
 *
 * @author Haocheng Wang Created on 2022/11/1
 */
public class XZ2PlusIndexStrategy extends IndexStrategy {

  private XZ2PCoding xz2PCoding;

  private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZ2PCoding.BYTES + MAX_OID_LENGTH + MAX_TID_LENGTH;
  private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES;
  private static final int SCAN_RANGE_BYTE_LEN =  PHYSICAL_KEY_BYTE_LEN - MAX_OID_LENGTH - MAX_TID_LENGTH;

  public XZ2PlusIndexStrategy() {
    indexType = IndexType.XZ2Plus;
    this.xz2PCoding = new XZ2PCoding();
  }

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    List<byte[]> elements = new LinkedList<>();
    // 3. xz2p code
    elements.add(xz2PCoding.code(trajectory.getLineString()).getBytes());
    // 4. oid
    elements.add(getObjectIDBytes(trajectory));
    elements.add(getTrajectoryIDBytes(trajectory));
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
    // 2. concat shard and xz2p coding.
    for (CodingRange xz2PRange : keyScanRange) {
      for (short shard = 0; shard < shardNum; shard++) {
        result.add(new RowKeyRange(new ByteArray(Arrays.asList(Bytes.toBytes(shard), xz2PRange.getLower().getBytes())),
            new ByteArray(Arrays.asList(Bytes.toBytes(shard), xz2PRange.getUpper().getBytes())), xz2PRange.isContained()));
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
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", indexId=" + getIndexType()
        + ", xz2P=" + extractSpatialCode(byteArray) + ", oidAndTid=" + getObjectID(byteArray) + "-" + getTrajectoryID(byteArray)
        + '}';
  }

  @Override
  public SpatialCoding getSpatialCoding() {
    return xz2PCoding;
  }

  @Override
  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    byte[] codingByteArray = new byte[XZ2PCoding.BYTES];
    buffer.get(codingByteArray);
    return new ByteArray(codingByteArray);
  }

  @Override
  public TimeCoding getTimeCoding() {
    return null;
  }

  @Override
  public TimeBin getTimeBin(ByteArray byteArray) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimeElementCode(ByteArray byteArray) {
    return 0;
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
    byte[] codingByteArray = new byte[XZ2PCoding.BYTES];
    buffer.get(codingByteArray);
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    byte[] codingByteArray = new byte[XZ2PCoding.BYTES];
    buffer.get(codingByteArray);
    byte[] oidBytes = new byte[MAX_OID_LENGTH];
    buffer.get(oidBytes);
    byte[] tidBytes = new byte[MAX_TID_LENGTH];
    buffer.get(tidBytes);
    return new String(tidBytes, StandardCharsets.UTF_8);
  }

  private List<CodingRange> getKeyScanRange(List<CodingRange> codingRanges) {
    List<CodingRange> codingRangesList = new ArrayList<>();
    for (CodingRange codingRange : codingRanges) {
      ByteArray lower = codingRange.getLower();
      ByteArray upper = codingRange.getUpper();
      // 被包含的索引区间，只精确到xz2编码即可
      if (codingRange.isContained()) {
        ByteBuffer byteBufferTemp = ByteBuffer.allocate(Long.BYTES);
        ByteBuffer byteBuffer = upper.toByteBuffer();
        ((Buffer)byteBuffer).flip();
        long xz2Coding = byteBuffer.getLong() + 1;
        byteBufferTemp.putLong(xz2Coding);
        ByteArray newUpper = new ByteArray(byteBufferTemp);
        codingRangesList.add(new CodingRange(lower, newUpper, codingRange.isContained()));
      } else { // 待精过滤的索引区间，xz2编码后还需要添加posCode。
        ByteBuffer byteBufferTemp = ByteBuffer.allocate(Long.BYTES + Byte.BYTES);
        ByteBuffer byteBuffer = upper.toByteBuffer();
        ((Buffer)byteBuffer).flip();
        long xz2Coding = byteBuffer.getLong();
        // TODO： bug， poscode = 15时，会越出范围，变为0。
        // TODO： 下面的这种情况，应该将xz2Coding增大1，posCode为0。
        byte posCode = byteBuffer.get();
        if (posCode == 15) {
          byteBufferTemp.putLong(xz2Coding + 1);
          byteBufferTemp.put((byte) 0);
        } else {
          byteBufferTemp.putLong(xz2Coding);
          byteBufferTemp.put((byte) (posCode + 1));
        }
        ByteArray newUpper = new ByteArray(byteBufferTemp);
        codingRangesList.add(new CodingRange(lower, newUpper, codingRange.isContained()));
      }
    }
    return codingRangesList;
  }
}
