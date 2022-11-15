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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_CF;
import static cn.edu.whu.trajspark.core.common.trajectory.Trajectory.Schema.*;

/**
 * Utils helps serialize/deserialize trajectory objects to/from HBase Put/Result.
 *
 * @author Haocheng Wang
 * Created on 2022/10/24
 */
public class TrajectorySerdeUtils {

  public static final byte[] COLUMN_FAMILY = Bytes.toBytes(DATA_TABLE_CF);
  public static final byte[] TRAJECTORY_ID_QUALIFIER = Bytes.toBytes(TRAJECTORY_ID);
  public static final byte[] OBJECT_ID_QUALIFIER = Bytes.toBytes(OBJECT_ID);
  public static final byte[] MBR_QUALIFIER = Bytes.toBytes(MBR);
  public static final byte[] START_POSITION_QUALIFIER = Bytes.toBytes(START_POSITION);
  public static final byte[] END_POSITION_QUALIFIER = Bytes.toBytes(END_POSITION);
  public static final byte[] START_TIME_QUALIFIER = Bytes.toBytes(START_TIME);
  public static final byte[] END_TIME_QUALIFIER = Bytes.toBytes(END_TIME);
  public static final byte[] POINT_NUMBER_QUALIFIER = Bytes.toBytes(POINT_NUMBER);
  public static final byte[] SPEED_QUALIFIER = Bytes.toBytes(SPEED);
  public static final byte[] LENGTH_QUALIFIER = Bytes.toBytes(LENGTH);
  public static final byte[] TRAJ_POINTS_QUALIFIER = Bytes.toBytes(TRAJ_POINTS);
  public static final byte[] SIGNATURE_QUALIFIER = Bytes.toBytes(SIGNATURE);
  public static final byte[] PTR_QUALIFIER = Bytes.toBytes(PTR);


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
    int pointNumber = result.getValue(COLUMN_FAMILY, POINT_NUMBER_QUALIFIER) == null ? -1 :
        (Integer) SerializerUtils.deserializeObject(result.getValue(COLUMN_FAMILY, POINT_NUMBER_QUALIFIER), Integer.class);
    double speed = result.getValue(COLUMN_FAMILY, SPEED_QUALIFIER) == null ? -1 :
        (Double) SerializerUtils.deserializeObject(result.getValue(COLUMN_FAMILY, SPEED_QUALIFIER), Double.class);
    double len = result.getValue(COLUMN_FAMILY, LENGTH_QUALIFIER) == null ? -1 :
        (Double) SerializerUtils.deserializeObject(result.getValue(COLUMN_FAMILY, LENGTH_QUALIFIER), Double.class);
    TrajFeatures trajectoryFeatures = new TrajFeatures(startTime, endTime, startPoint,
        endPoint, pointNumber, mbr, speed, len);
    trajectory.setTrajectoryFeatures(trajectoryFeatures);
  }


  private static void setTrajPointList(Result mainIndexResult, Trajectory trajectory) throws IOException {
    List<TrajPoint> list = SerializerUtils.deserializeList(
        mainIndexResult.getValue(COLUMN_FAMILY, TRAJ_POINTS_QUALIFIER),
          TrajPoint.class);
    trajectory.setPointList(list);
  }

  public static Trajectory getTrajectory(Result result) throws IOException {
    Trajectory trajectory = new Trajectory();
    setBasicTrajectoryInfos(result, trajectory);
    setTrajPointList(result, trajectory);
    return trajectory;
  }

  public static Trajectory getTrajectory(byte[] trajPointsByteArray, byte[] objectID, byte[] tidBytes) throws IOException {
    List<TrajPoint> trajPointList = SerializerUtils.deserializeList(trajPointsByteArray, TrajPoint.class);
    String objectStr = (String) SerializerUtils.deserializeObject(objectID, String.class);
    String tidStr = (String) SerializerUtils.deserializeObject(tidBytes, String.class);
    return new Trajectory(tidStr, objectStr, trajPointList);
  }

  public static TrajFeatures getTrajectoryFeatures(Result result) throws IOException {
    Trajectory trajectory = new Trajectory();
    setBasicTrajectoryInfos(result, trajectory);
    return trajectory.getTrajectoryFeatures();
  }

  public static byte[] getByteArrayByQualifier(Result result, byte[] qualifier) {
    return result.getValue(COLUMN_FAMILY, qualifier);
  }

  public static boolean isMainIndexed(Result result) {
    return result.getValue(COLUMN_FAMILY, PTR_QUALIFIER) == null;
  }
}
