package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.BasicFilterConfig;
import cn.edu.whu.trajspark.core.util.GeoUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/9/27
 **/
public class BasicFilter implements IFilter {
  private double maxSpeed;
  private double minTrajLength;

  /**
   * 基础去噪
   *
   * @param maxSpeed      速度阈值，单位：km/h
   * @param minTrajLength 最小轨迹长度，单位：m
   */
  public BasicFilter(double maxSpeed, double minTrajLength) {
    this.maxSpeed = maxSpeed;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 使用配置文件构造
   * @param config
   */
  public BasicFilter(BasicFilterConfig config) {
    this.maxSpeed = config.getMaxSpeed();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
    // 时间去重
    Set<TrajPoint> tmpSet = new TreeSet<>(Comparator.comparing(TrajPoint::getTimestamp));
    tmpSet.addAll(rawTrajectory.getPointList());
    List<TrajPoint> tmpPointList = new ArrayList<>(tmpSet);
    // 离群点剔除
    for (int i = 1; i < tmpPointList.size(); ++i) {
      TrajPoint p0 = tmpPointList.get(i - 1), p1 = tmpPointList.get(i);
      if (GeoUtils.getSpeed(p0, p1) >= maxSpeed) {
        tmpPointList.remove(i);
        i -= 1;
      }
    }
    Trajectory cleanedTrajtroy =
        new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(),
            tmpPointList, rawTrajectory.getExtendedValues());
    return cleanedTrajtroy.getTrajectoryFeatures().getLen() > minTrajLength ? cleanedTrajtroy :
        null;
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction);
  }
}
