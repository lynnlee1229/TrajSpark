package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.segmenter.BasicSegmenterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.CountSegmenterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.ISegmenterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.StayPointBasedSegmenterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.UserDefinedSegmenterConfig;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

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

  static ISegmenter getSegmenter(ISegmenterConfig config) {
    switch (config.getSegmenterType()) {
      case BASIC_SEGMENTER:
        if (config instanceof BasicSegmenterConfig) {
          return new BasicSegmenter((BasicSegmenterConfig) config);
        }
        throw new NoSuchMethodError();
      case STAYPOINTBASED_SEGMENTER:
        if (config instanceof StayPointBasedSegmenterConfig) {
          return new StayPointBasedSegmenter((StayPointBasedSegmenterConfig) config);
        }
        throw new NoSuchMethodError();
      case USERDEFINED_SEGMENTER:
        if (config instanceof UserDefinedSegmenterConfig) {
          return new UserDefinedSegmenter((UserDefinedSegmenterConfig) config);
        }
        throw new NoSuchMethodError();
      case COUNT_SEGMENTER:
        if (config instanceof CountSegmenterConfig) {
          return new CountSegmenter((CountSegmenterConfig) config);
        }
        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }

}
