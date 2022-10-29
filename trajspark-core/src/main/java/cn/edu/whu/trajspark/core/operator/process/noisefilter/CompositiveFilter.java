package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.CompositiveFilterConfig;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/29
 **/
public class CompositiveFilter implements IFilter {
  private BasicFilter basicFilter;
  private PingpongFilter pingpongFilter;


  /**
   * 集成去噪
   *
   * @param basicFilter    离群点去除
   * @param pingpongFilter 乒乓效应去除
   */
  public CompositiveFilter(BasicFilter basicFilter, PingpongFilter pingpongFilter) {
    this.basicFilter = basicFilter;
    this.pingpongFilter = pingpongFilter;
  }

  /**
   * 通过配置文件构造
   *
   * @param config
   */
  public CompositiveFilter(CompositiveFilterConfig config) {
    this.basicFilter = new BasicFilter(config.getBasicFilterConfig());
    this.pingpongFilter = new PingpongFilter(config.getPingpongFilterConfig());
  }

  /**
   * 通过参数构造
   *
   * @param maxSpeed         速度阈值，单位：km/h
   * @param baseStationIndex 基站ID索引名
   * @param maxPingpongTime  时间阈值，在该阈值内"A-B-A"被视作乒乓效应
   * @param minTrajLength    最小轨迹长度，单位：km
   */
  public CompositiveFilter(double maxSpeed, String baseStationIndex, double maxPingpongTime,
                           double minTrajLength) {
    this.basicFilter = new BasicFilter(maxSpeed, minTrajLength);
    this.pingpongFilter = new PingpongFilter(baseStationIndex, maxPingpongTime, minTrajLength);
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
    return basicFilter.filterFunction(pingpongFilter.filterFunction(rawTrajectory));
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction);
  }
}
