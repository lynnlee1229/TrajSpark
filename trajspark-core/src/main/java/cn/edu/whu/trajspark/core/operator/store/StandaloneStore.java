package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.StandaloneStoreConfig;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.TrajectoryConvertor;
import cn.edu.whu.trajspark.core.util.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

/**
 * @author Lynn Lee
 * @date 2022/9/21
 **/
public class StandaloneStore implements IStore {

  private StandaloneStoreConfig storeConfig;

  StandaloneStore(StandaloneStoreConfig storeConfig) {
    this.storeConfig = storeConfig;
  }

  public void storePointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) {
    trajectoryJavaRDD.foreach(
        item -> {
          String fileName =
              String.format("%s/%s-%s.csv",
                  storeConfig.getLocation(),
                  item.getObjectID(),
                  item.getTrajectoryID());
          String outputString = TrajectoryConvertor.convert(item);
          IOUtils.writeStringToFile(fileName, outputString);
        }
    );
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

  public void storeStayPoint(JavaRDD<StayPoint> stayPointJavaRDD) {
    // TODO stayPoint 解析
//    stayPointJavaRDD.mapToPair((stayPoint) -> {
//          String record =
//              stayPoint.getSid() + "," + stayPoint.getOid() + "," + stayPoint.getStartTime() + "," +
//                  stayPoint.getEndTime() + ",'" + stayPoint.getCenterPoint() + "'";
//          return new Tuple2(stayPoint.getSid(), record);
//        }).persist(StorageLevels.MEMORY_AND_DISK)
//        .saveAsHadoopFile(this.storeConfig.getLocation(), String.class, String.class,
//            RDDMultipleTextOutputFormat.class);
  }

}
