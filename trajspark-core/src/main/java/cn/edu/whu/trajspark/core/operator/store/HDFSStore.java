package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.HDFSStoreConfig;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.TrajectoryConvertor;
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
    JavaPairRDD<String, String> cachedRDD = trajectoryJavaRDD.mapToPair(
        item -> {
          String fileName;
          if (item.getObjectID().length() == 0) {
            fileName =
                String.format("%s%s",
                    item.getTrajectoryID(),
                    storeConfig.getFilePostFix());
          } else {
            fileName =
                String.format("%s/%s%s",
                    item.getObjectID(),
                    item.getTrajectoryID(),
                    storeConfig.getFilePostFix());
          }
          String outputString = TrajectoryConvertor.convert(item, storeConfig.getSplitter());
          return new Tuple2<>(fileName, outputString);
        }
    ).persist(StorageLevels.MEMORY_AND_DISK);
    Map<String, Long> keyCountedResult = cachedRDD.countByKey();
    cachedRDD.partitionBy(new HashPartitioner(keyCountedResult.size()))
        .saveAsHadoopFile(storeConfig.getLocation(), String.class, String.class,
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
      return key;
    }

    protected String generateActualKey(String key, String value) {
      return super.generateActualKey(null, value);
    }
  }
}
