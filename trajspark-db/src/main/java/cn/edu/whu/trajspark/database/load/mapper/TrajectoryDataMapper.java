package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.mapper.datatypes.KeyFamilyQualifier;
import cn.edu.whu.trajspark.database.load.mapper.datatypes.KeyValueInfo;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static cn.edu.whu.trajspark.base.trajectory.Trajectory.Schema.*;
import static cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils.COLUMN_FAMILY;
import static cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils.PTR_QUALIFIER;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TrajectoryDataMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrajectoryDataMapper.class);

  private static byte[] mainIndexKey;

  public static byte[] getMapRowKey(Trajectory trajectory, IndexMeta indexMeta) {
    ByteArray index = indexMeta.getIndexStrategy().index(trajectory);
    return index.getBytes();
  }

  /**
   * Convert a trajectory to a put object based on index information
   *
   * @param trajectory trajectory
   * @param indexMeta  index information
   * @return hbase put
   * @throws IOException ..
   */

  public static Put getMapPut(Trajectory trajectory, IndexMeta indexMeta) throws IOException {
    Put put = null;
    if (indexMeta.isMainIndex()) {
      put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
      mainIndexKey = put.getRow();
    } else {
      put = TrajectorySerdeUtils.getPutForSecondaryIndex(indexMeta, trajectory, mainIndexKey);
    }
    return put;
  }

  public static void configureHFilesOnHDFS(Database instance, String tableName, Job job)
      throws IOException {
    Table table = instance.getTable(tableName);
    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);
    HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
  }

  public static List<Put> mapTrajectoryToRow(Trajectory trajectory, List<IndexMeta> indexMetaList)
      throws IOException {
    ArrayList<Put> putArrayList = new ArrayList<>();
    for (IndexMeta indexMeta : indexMetaList) {
      final byte[] rowKey = getMapRowKey(trajectory, indexMeta);
      Put put = getMapPut(trajectory, indexMeta);
      if (rowKey == null || rowKey.length <= 0) {
        LOGGER.info("Trajectory Key is invalid");
      } else {
        putArrayList.add(put);
      }
    }
    return putArrayList;
  }

  public static List<Tuple2<KeyFamilyQualifier, KeyValue>> mapPutToKeyValue(Put put)
      throws IOException {
    List<Tuple2<KeyFamilyQualifier, KeyValue>> value = new ArrayList<>();
    String[] dataFrame;
    if (put.has(COLUMN_FAMILY, PTR_QUALIFIER)) {
      dataFrame = new String[]{PTR};
    } else {
      dataFrame = new String[]{
          TRAJECTORY_ID, OBJECT_ID, TRAJ_POINTS, MBR, START_TIME, END_TIME, START_POSITION,
          END_POSITION, POINT_NUMBER, SPEED, LENGTH};
    }
    for (String frame : dataFrame) {
      List<Cell> cells = put.get(COLUMN_FAMILY, Bytes.toBytes(frame));
      Result result = Result.create(cells);
      byte[] quaFilterValue = result.getValue(COLUMN_FAMILY, Bytes.toBytes(frame));
      KeyValue keyValue = new KeyValue(put.getRow(), COLUMN_FAMILY, Bytes.toBytes(frame),
          quaFilterValue);
      KeyFamilyQualifier keyFamilyQualifier = new KeyFamilyQualifier(result.getRow(), COLUMN_FAMILY,
          Bytes.toBytes(frame));
      value.add(new Tuple2<>(keyFamilyQualifier, keyValue));
    }
    return value;
  }

  public static List<KeyValue> mapPutToSortedKeyValue(Put put) {
    List<KeyValueInfo> value = new ArrayList<>();
    List<KeyValue> sortedValue = new ArrayList<>();
    String[] dataFrame = {
        TRAJECTORY_ID, OBJECT_ID, TRAJ_POINTS, MBR, START_TIME, END_TIME, START_POSITION,
        END_POSITION, POINT_NUMBER, SPEED, LENGTH};
    for (String frame : dataFrame) {
      List<Cell> cells = put.get(COLUMN_FAMILY, Bytes.toBytes(frame));
      Result result = Result.create(cells);
      byte[] quaFilterValue = result.getValue(COLUMN_FAMILY, Bytes.toBytes(frame));
      KeyValue keyValue = new KeyValue(put.getRow(), COLUMN_FAMILY, Bytes.toBytes(frame),
          quaFilterValue);
      value.add(new KeyValueInfo(frame, keyValue));
    }
    value.sort(Comparator.comparing(KeyValueInfo::getQualifier));
    value.forEach(System.out::println);
    for (KeyValueInfo keyValueInfo : value) {
      sortedValue.add(keyValueInfo.getValue());
    }
    return sortedValue;
  }

  public static Trajectory mapHBaseResultToTrajectory(Result result) throws IOException {
    return TrajectorySerdeUtils.mainRowToTrajectory(result);
  }

}
