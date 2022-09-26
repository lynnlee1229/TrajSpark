package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/9/26
 **/
public interface ISegmenter extends Serializable {
  /**
   * 分段函数，适用RDD.flatmap(func)形式调用
   *
   * @param rawTrajectory 未分段轨迹
   * @return 子轨迹集合
   */
  List<Trajectory> segmentFunction(Trajectory rawTrajectory);

  /**
   * 分段函数，适用.segment(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<未分段轨迹>
   * @return RDD<子轨迹>
   */
  JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD);

}
