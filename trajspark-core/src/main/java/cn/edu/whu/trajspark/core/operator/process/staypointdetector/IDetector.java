package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.BasePoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;

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
  List<BasePoint> detectFunction(Trajectory rawTrajectory);

  /**
   * 停留检测函数，适用.detect(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<原始轨迹>
   * @return RDD<停留点>
   */
  JavaRDD<BasePoint> detect(JavaRDD<Trajectory> rawTrajectoryRDD);

}
