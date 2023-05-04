package cn.edu.whu.trajspark.core.operator.process.simplifier;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.simplifier.DPSimplifierConfig;
import cn.edu.whu.trajspark.core.conf.process.simplifier.ISimplifierConfig;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
public interface ISimplifier extends Serializable {
  /**
   * 化简函数，适用RDD.map(func)形式调用
   *
   * @param rawTrajectory 未化简轨迹
   * @return 化简后轨迹
   */
  Trajectory simplifyFunction(Trajectory rawTrajectory);

  /**
   * 化简函数，适用.simplify(RDD)形式调用
   *
   * @param rawTrajectoryRDD RDD<未化简轨迹>
   * @return RDD<过滤后轨迹>
   */
  JavaRDD<Trajectory> simplify(JavaRDD<Trajectory> rawTrajectoryRDD);

  static ISimplifier getSimplifier(ISimplifierConfig config) {
    switch (config.getSimplifierType()) {
      case DP_SIMPLIFIER:
        if (config instanceof DPSimplifierConfig) {
          return new DPSimplifier((DPSimplifierConfig) config);
        }
        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }
}
