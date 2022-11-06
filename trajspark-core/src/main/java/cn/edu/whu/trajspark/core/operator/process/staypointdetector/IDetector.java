package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.detector.BasicDetectorConfig;
import cn.edu.whu.trajspark.core.conf.process.detector.IDetectorConfig;
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
   * 停留检测函数，为基于停留的分段服务
   * @param rawTrajectory 原始轨迹
   * @return 添加stop、move语义标签的轨迹
   */
  Trajectory detectFunctionForSegmenter(Trajectory rawTrajectory);

  /**
   * 停留检测函数，适用.detect(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<原始轨迹>
   * @return RDD<停留点>
   */
  JavaRDD<StayPoint> detect(JavaRDD<Trajectory> rawTrajectoryRDD);

  /**
   * 返回停留标签名
   * @return 停留标签名
   */
  String getStayPointTagName();
  static IDetector getDector(IDetectorConfig config) {
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
