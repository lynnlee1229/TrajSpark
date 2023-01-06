package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.CompositiveFilterConfig;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/29
 **/
public class CompositiveFilter implements IFilter {
  private PingpongFilter pingpongFilter;

  private DriftFilter driftFilter;

  /**
   * 集成去噪
   *
   * @param pingpongFilter 乒乓效应去噪
   * @param driftFilter    离群点去噪
   */
  public CompositiveFilter(PingpongFilter pingpongFilter, DriftFilter driftFilter) {
    this.pingpongFilter = pingpongFilter;
    this.driftFilter = driftFilter;
  }


  /**
   * 通过配置文件构造
   *
   * @param config
   */
  public CompositiveFilter(CompositiveFilterConfig config) {
    this.driftFilter = new DriftFilter(config.getDriftFilterConfig());
    this.pingpongFilter = new PingpongFilter(config.getPingpongFilterConfig());
  }


  /**
   * 通过参数构造
   * @param maxSpeed 速度阈值，单位：km/h
   * @param minAlpha 角度阈值，单位：度
   * @param maxRatio 距离位移比，无量纲
   * @param baseStationIndex 基站ID索引位
   * @param maxPingpongTime 乒乓效应判定时间阈值，单位：s
   * @param minTrajLength 最小轨迹长度，单位：km
   */
  public CompositiveFilter(double maxSpeed, double minAlpha, double maxRatio,
                           String baseStationIndex, double maxPingpongTime, double minTrajLength) {
    this.driftFilter = new DriftFilter(maxSpeed, minAlpha, maxRatio, minTrajLength);
    this.pingpongFilter = new PingpongFilter(baseStationIndex, maxPingpongTime, minTrajLength);
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
//    return driftFilter.filterFunction(pingpongFilter.filterFunction(rawTrajectory));
    return pingpongFilter.filterFunction(driftFilter.filterFunction(rawTrajectory));
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction);
  }
}
