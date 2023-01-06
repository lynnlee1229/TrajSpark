package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.UserDefinedFilterConfig;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/11/15
 **/
public class UserDefinedFilter implements IFilter {
  private List<IFilter> filterList;

  /**
   * 用户定义去噪器，由用户组织去噪流程
   *
   * @param filterList 传入去噪器列表，列表中顺序即为执行顺序
   */
  public UserDefinedFilter(List<IFilter> filterList) {
    this.filterList = filterList;
  }

  /**
   * 通过配置文件初始化
   *
   * @param config
   */
  public UserDefinedFilter(UserDefinedFilterConfig config) {
    this.filterList =
        config.getFilterConfigList().stream().map(IFilter::getFilter).collect(Collectors.toList());
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
    for (IFilter iFilter : filterList) {
      rawTrajectory = iFilter.filterFunction(rawTrajectory);
    }
    return rawTrajectory;
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction);
  }
}
