package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public interface IFilter extends Serializable {
  /**
   * 过滤函数，适用RDD.map(func)形式调用
   *
   * @param rawTrajectory 未过滤轨迹
   * @return 过滤后轨迹
   */
  Trajectory filterFunction(Trajectory rawTrajectory);

  /**
   * 过滤函数，适用.filter(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<未过滤轨迹>
   * @return RDD<过滤后轨迹>
   */
  JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD);

}
