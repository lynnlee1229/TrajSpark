package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.BasicFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.CompositiveFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.DriftFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.IFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.PingpongFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.UserDefinedFilterConfig;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

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

  static IFilter getFilter(IFilterConfig config) {
    switch (config.getFilterType()) {
      case BASIC_FILTER:
        if (config instanceof BasicFilterConfig) {
          return new BasicFilter((BasicFilterConfig) config);
        }
        throw new NoSuchMethodError();
      case PINGPONG_FILTER:
        if (config instanceof PingpongFilterConfig) {
          return new PingpongFilter((PingpongFilterConfig) config);
        }
        throw new NoSuchMethodError();
      case DRIFT_FILTER:
        if (config instanceof DriftFilterConfig) {
          return new DriftFilter((DriftFilterConfig) config);
        }
        throw new NoSuchMethodError();
      case COMPOSITIVE_FILTER:
        if (config instanceof CompositiveFilterConfig) {
          return new CompositiveFilter((CompositiveFilterConfig) config);
        }
        throw new NoSuchMethodError();
      case USERDEFINED_FILTER:
        if (config instanceof UserDefinedFilterConfig) {
          return new UserDefinedFilter((UserDefinedFilterConfig) config);
        }
        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }
}
