package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.HDFSStoreConfig;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import scala.NotImplementedError;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @author Lynn Lee
 * @date 2022/9/21
 **/
public class HDFSStore implements IStore {
  private static final Logger LOGGER = Logger.getLogger(HDFSStore.class);
  private HDFSStoreConfig storeConfig;

  HDFSStore(HDFSStoreConfig storeConfig) {
    this.storeConfig = storeConfig;
  }

  public void storePointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) {
    LOGGER.info("Storing BasePointTrajectory into location : " + this.storeConfig.getLocation());
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    JavaPairRDD<String, String> cachedRDD = trajectoryJavaRDD.flatMap((traj) -> {
      List<Tuple3<String, String, TrajPoint>>
          trajIdAndTrajPoint = new ArrayList(traj.getPointList().size());
      Iterator plistIter = traj.getPointList().iterator();

      while (plistIter.hasNext()) {
        TrajPoint p = (TrajPoint) plistIter.next();
        trajIdAndTrajPoint.add(new Tuple3(traj.getTrajectoryID(), traj.getObjectID(), p));
      }

      return trajIdAndTrajPoint.iterator();
    }).mapToPair((trajIdAndGpsPoint) -> {
      String tid = trajIdAndGpsPoint._1();
      String oid = trajIdAndGpsPoint._2();
      TrajPoint tmpP = trajIdAndGpsPoint._3();
      StringBuilder record = new StringBuilder();
      record.append(tid).append(",");
      record.append(oid).append(",");
      record.append(tmpP.getPid()).append(",");
      record.append(tmpP.getLat()).append(",");
      record.append(tmpP.getLng()).append(",");
      record.append(tmpP.getTimestamp().format(formatter).toString());
      if (null != tmpP.getExtendedValues()) {
        Iterator pointIter =
            tmpP.getExtendedValues().entrySet().iterator();

        while (pointIter.hasNext()) {
          Map.Entry<String, Object> set = (Map.Entry) pointIter.next();
          record.append(",").append(set.getValue());
        }
      }

      return new Tuple2(oid + "/" + oid + "-" + tid, record.toString());
    }).persist(StorageLevels.MEMORY_AND_DISK);
    Map<String, Long> keyCountResult = cachedRDD.countByKey();
    cachedRDD.partitionBy(new HashPartitioner(keyCountResult.size()))
        .saveAsHadoopFile(this.storeConfig.getLocation(), String.class, String.class,
            RDDMultipleTextOutputFormat.class);
    cachedRDD.unpersist();
  }


  public void storeTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) {
    switch (this.storeConfig.getSchema()) {
      case POINT_BASED_TRAJECTORY:
        this.storePointBasedTrajectory(trajectoryJavaRDD);
        return;
      default:
        throw new NotImplementedError();
    }
  }

  @Override
  public void storeStayPointList(JavaRDD<List<StayPoint>> spList) {
  }

  @Override
  public void storeStayPointASTraj(JavaRDD<StayPoint> sp) {
  }

  public void storeStayPoint(JavaRDD<StayPoint> stayPointJavaRDD) {
    stayPointJavaRDD.mapToPair((stayPoint) -> {
          String record =
              stayPoint.getSid() + "," + stayPoint.getOid() + "," + stayPoint.getStartTime() + ","
                  + stayPoint.getEndTime() + ",'" + stayPoint.getCenterPoint() + "'";
          return new Tuple2(stayPoint.getSid(), record);
        }).persist(StorageLevels.MEMORY_AND_DISK)
        .saveAsHadoopFile(this.storeConfig.getLocation(), String.class, String.class,
            RDDMultipleTextOutputFormat.class);
  }

  public static class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String, String> {
    public RDDMultipleTextOutputFormat() {
    }

    public String generateFileNameForKeyValue(String key, String value, String name) {
      return key + ".csv";
    }

    protected String generateActualKey(String key, String value) {
      return (String) super.generateActualKey(null, value);
    }
  }
}
