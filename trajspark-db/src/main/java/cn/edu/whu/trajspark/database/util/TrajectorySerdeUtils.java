package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.core.common.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.util.SerializerUtils;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.IndexStrategy;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_CF;
import static cn.edu.whu.trajspark.core.common.trajectory.Trajectory.Schema.*;

/**
 * Utils helps serialize/deserialize trajectory objects to/from HBase Put/Result.
 *
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class TrajectorySerdeUtils {

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes(DATA_TABLE_CF);
  private static final byte[] TRAJECTORY_ID_QUALIFIER = Bytes.toBytes(TRAJECTORY_ID);
  private static final byte[] OBJECT_ID_QUALIFIER = Bytes.toBytes(OBJECT_ID);
  private static final byte[] MBR_QUALIFIER = Bytes.toBytes(MBR);
  private static final byte[] START_POSITION_QUALIFIER = Bytes.toBytes(START_POSITION);
  private static final byte[] END_POSITION_QUALIFIER = Bytes.toBytes(END_POSITION);
  private static final byte[] START_TIME_QUALIFIER = Bytes.toBytes(START_TIME);
  private static final byte[] END_TIME_QUALIFIER = Bytes.toBytes(END_TIME);
  private static final byte[] POINT_NUMBER_QUALIFIER = Bytes.toBytes(POINT_NUMBER);
  private static final byte[] SPEED_QUALIFIER = Bytes.toBytes(SPEED);
  private static final byte[] LENGTH_QUALIFIER = Bytes.toBytes(LENGTH);
  private static final byte[] TRAJ_POINTS_QUALIFIER = Bytes.toBytes(TRAJ_POINTS);
  private static final byte[] SIGNATURE_QUALIFIER = Bytes.toBytes(SIGNATURE);
  private static final byte[] PTR_QUALIFIER = Bytes.toBytes(PTR);


  /**
   * Get an main index, so TRAJ_POINTS is not null, but PTR is null
   */
  public static Put getPutForMainIndex(IndexMeta indexMeta, Trajectory trajectory) throws IOException {
    Put put = new Put(getRowKey(indexMeta.getIndexStrategy(), trajectory));
    addBasicTrajectoryInfos(put, trajectory);
    put.addColumn(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER,
        SerializerUtils.serializeList(trajectory.getPointList(), TrajPoint.class));
    return put;
  }

  /**
   * Get a secondary index put, so TRAJ_POINTS is null, but PTR points to a main index row key byte array.
   */
  public static Put getPutForSecondaryIndex(IndexMeta indexMeta, Trajectory trajectory, byte[] ptr) throws IOException {
    Put put = new Put(getRowKey(indexMeta.getIndexStrategy(), trajectory));
    addBasicTrajectoryInfos(put, trajectory);
    put.addColumn(COLUMN_FAMILY, PTR_QUALIFIER, ptr);
    return put;
  }

  private static byte[] getRowKey(IndexStrategy indexStrategy, Trajectory trajectory) {
    ByteArray byteArray = indexStrategy.index(trajectory);
    return byteArray.getBytes();
  }

  /**
   * Add basic columns (except for TRAJ_POINTS, PTR, SIGNATURE) into put object
   */
  private static void addBasicTrajectoryInfos(Put put, Trajectory trajectory) throws IOException {
    TrajFeatures trajectoryFeatures = trajectory.getTrajectoryFeatures();
    put.addColumn(COLUMN_FAMILY, TRAJECTORY_ID_QUALIFIER,
        SerializerUtils.serializeObject(trajectory.getTrajectoryID()));
    put.addColumn(COLUMN_FAMILY, OBJECT_ID_QUALIFIER,
        SerializerUtils.serializeObject(trajectory.getObjectID()));
    put.addColumn(COLUMN_FAMILY, MBR_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getMbr()));
    put.addColumn(COLUMN_FAMILY, START_POSITION_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getStartPoint()));
    put.addColumn(COLUMN_FAMILY, END_POSITION_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getEndPoint()));
    put.addColumn(COLUMN_FAMILY, START_TIME_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getStartTime()));
    put.addColumn(COLUMN_FAMILY, END_TIME_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getEndTime()));
    put.addColumn(COLUMN_FAMILY, POINT_NUMBER_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getPointNum()));
    put.addColumn(COLUMN_FAMILY, SPEED_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getSpeed()));
    put.addColumn(COLUMN_FAMILY, LENGTH_QUALIFIER,
        SerializerUtils.serializeObject(trajectoryFeatures.getLen()));
  }

  /**
   * Extract basic columns (except for TRAJ_POINTS, PTR, SIGNATURE) from result into trajectory object.
   */
  private static void setBasicTrajectoryInfos(Result result, Trajectory trajectory) {
    trajectory.setTrajectoryID(
        (String) SerializerUtils.deserializeObject(
            result.getValue(COLUMN_FAMILY, TRAJECTORY_ID_QUALIFIER),
            String.class));
    trajectory.setObjectID(
        (String) SerializerUtils.deserializeObject(
            result.getValue(COLUMN_FAMILY, OBJECT_ID_QUALIFIER),
            String.class));
    MinimumBoundingBox mbr = (MinimumBoundingBox) SerializerUtils.deserializeObject(
            result.getValue(COLUMN_FAMILY, MBR_QUALIFIER),
        MinimumBoundingBox.class);
    TrajPoint startPoint = (TrajPoint) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, START_POSITION_QUALIFIER),
        TrajPoint.class);
    TrajPoint endPoint = (TrajPoint) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, END_POSITION_QUALIFIER),
        TrajPoint.class);
    ZonedDateTime startTime = (ZonedDateTime) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, START_TIME_QUALIFIER),
        ZonedDateTime.class);
    ZonedDateTime endTime = (ZonedDateTime) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, END_TIME_QUALIFIER),
        ZonedDateTime.class);
    Integer pointNumber = (Integer) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, POINT_NUMBER_QUALIFIER),
        Integer.class);
    Double speed = (Double) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, SPEED_QUALIFIER),
        Double.class);
    Double len = (Double) SerializerUtils.deserializeObject(
        result.getValue(COLUMN_FAMILY, LENGTH_QUALIFIER),
        Double.class);
    TrajFeatures trajectoryFeatures = new TrajFeatures(startTime, endTime, startPoint,
        endPoint, pointNumber, mbr, speed, len);
    trajectory.setTrajectoryFeatures(trajectoryFeatures);
  }

  /**
   * Set TrajPoint list of trajectory using the row got from hbase and the data table.
   *
   * @param secondaryIndexResult A row in HBase which stores the trajectory's info.
   * @param trajectory Trajectory object needed to set TrajPoint list.
   * @param dataTable A HBase table storing all indexes(main or secondary) of the data set.
   */
  private static void setTrajPointList(Result secondaryIndexResult, Trajectory trajectory, DataTable dataTable) throws IOException {
    boolean isMainIndexRow = secondaryIndexResult.getColumnLatestCell(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER) != null;
    if (isMainIndexRow) {
      setTrajPointList(secondaryIndexResult, trajectory);
    } else {
      // get the main index row key from PTR column, and use the new row to set TrajPoint list.
      byte[] mainIndexRowKey = secondaryIndexResult.getValue(COLUMN_FAMILY, PTR_QUALIFIER);
      Result result1 = dataTable.getTable().get(new Get(mainIndexRowKey));
      setTrajPointList(result1, trajectory);
    }
  }

  private static void setTrajPointList(Result mainIndexResult, Trajectory trajectory) throws IOException {
    List<TrajPoint> list = SerializerUtils.deserializeList(
        mainIndexResult.getValue(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER),
          TrajPoint.class);
    trajectory.setPointList(list);
  }

  public static Trajectory getTrajectory(Result result, DataTable table) throws IOException {
    Trajectory trajectory = new Trajectory();
    setBasicTrajectoryInfos(result, trajectory);
    setTrajPointList(result, trajectory, table);
    return trajectory;
  }

}
