package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.detector.BasicDetectorConfig;
import cn.edu.whu.trajspark.core.conf.process.detector.IDectorConfig;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

/**
 * @author Lynn Lee
 * @date 2022/9/26
 **/
public interface IDetector extends Serializable {
  /**
   * 停留检测函数，适用RDD.detect(func)形式调用
   *
   * @param rawTrajectory 原始轨迹
   * @return 停留点集合
   */
  List<StayPoint> detectFunction(Trajectory rawTrajectory);

  /**
   * 停留检测函数，适用.detect(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<原始轨迹>
   * @return RDD<停留点>
   */
  JavaRDD<StayPoint> detect(JavaRDD<Trajectory> rawTrajectoryRDD);

  static IDetector getDector(IDectorConfig config) {
    switch (config.getDetectorType()) {
      case BASIC_DETECTOR:
        if (config instanceof BasicDetectorConfig) {
          return new BasicDector((BasicDetectorConfig) config);
        }
        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }
}
