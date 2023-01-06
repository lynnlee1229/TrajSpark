package cn.edu.whu.trajspark.core.operator.store;

import static cn.edu.whu.trajspark.core.enums.StoreSchemaEnum.POINT_BASED_TRAJECTORY;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.StandaloneStoreConfig;
import cn.edu.whu.trajspark.core.enums.StoreSchemaEnum;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.StayPointConvertor;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.TrajectoryConvertor;
import cn.edu.whu.trajspark.core.util.IOUtils;
import java.util.List;
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
              String.format("%s/%s-%s%s",
                  storeConfig.getLocation(),
                  item.getObjectID(),
                  item.getTrajectoryID(),
                  storeConfig.getFilePostFix());
          String outputString = TrajectoryConvertor.convert(item, storeConfig.getSplitter(),
              storeConfig.getLineBreaker());
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

  // TODO 单stayPoint 解析
  public void storeStayPointList(JavaRDD<List<StayPoint>> stayPointJavaRDD) {
    stayPointJavaRDD.foreach(
        s -> {
          if (!s.isEmpty()) {
            String outputString = StayPointConvertor.convertSPList(s, storeConfig.getSplitter(),
                storeConfig.getLineBreaker());
            String fileName =
                String.format("%s/%s-splist%s",
                    storeConfig.getLocation(),
                    s.get(0).getSid().split("_")[0],
                    storeConfig.getFilePostFix()
                );
            IOUtils.writeStringToFile(fileName, outputString);
          }

        }
    );
  }

  public void storeStayPointASTraj(JavaRDD<StayPoint> stayPointJavaRDD) {
    stayPointJavaRDD.foreach(
        s -> {
          String outputString = StayPointConvertor.convertSPAsTraj(s, storeConfig.getSplitter(),
              storeConfig.getLineBreaker());
          String fileName =
              String.format("%s/%s-splist%s",
                  storeConfig.getLocation(),
                  s.getSid(),
                  storeConfig.getFilePostFix());
          IOUtils.writeStringToFile(fileName, outputString);
        }
    );
  }

}
