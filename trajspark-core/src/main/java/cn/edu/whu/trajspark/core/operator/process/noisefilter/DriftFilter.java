package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.DriftFilterConfig;
import cn.edu.whu.trajspark.base.util.GeoUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/11/6
 **/
public class DriftFilter implements IFilter {
  private double maxSpeed;
  private double minAlpha;

  private double maxRatio;
  private double minTrajLength;

  /**
   * 漂移点过滤
   *
   * @param maxSpeed      速度阈值，单位：km/h
   * @param minAlpha      角度阈值，单位：度
   * @param maxRatio      距离与位移比值，无单位
   * @param minTrajLength 最短轨迹长度，单位：km
   */
  public DriftFilter(double maxSpeed, double minAlpha, double maxRatio, double minTrajLength) {
    this.maxSpeed = maxSpeed;
    this.minAlpha = minAlpha;
    this.maxRatio = maxRatio;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 通过配置文件初始化
   *
   * @param config
   */
  public DriftFilter(DriftFilterConfig config) {
    this.maxSpeed = config.getMaxSpeed();
    this.minAlpha = config.getMinAlpha();
    this.maxRatio = config.getMaxRatio();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
    // 1 时间去重
    Set<TrajPoint> tmpSet = new TreeSet<>(Comparator.comparing(TrajPoint::getTimestamp));
    tmpSet.addAll(rawTrajectory.getPointList());
    List<TrajPoint> tmpPointList = new ArrayList<>(tmpSet);
    // 2 离群点剔除
    for (int i = 1; i < tmpPointList.size() - 1; ++i) {
      boolean removeI = false;
      // 取出i-1、i、i+1三点进行规则判断
      TrajPoint p0 = tmpPointList.get(i - 1), p1 = tmpPointList.get(i), p2 =
          tmpPointList.get(i + 1);
      // 规则1：速度超限
      if ((GeoUtils.getSpeed(p0, p1) + GeoUtils.getSpeed(p1, p2)) / 2 >= maxSpeed) {
        removeI = true;
      }
      // 规则2：角度超限
      if (GeoUtils.getAngle(p0, p1, p2) <= minAlpha) {
        removeI = true;
      }
      // 规则3：距离位移比超限
      if (GeoUtils.getRatio(p0, p1, p2) >= maxRatio) {
        removeI = true;
      }
      if (removeI) {
        tmpPointList.remove(i);
        i -= 1;
      }
    }
    Trajectory cleanedTrajtroy =
        new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(), tmpPointList,
            rawTrajectory.getExtendedValues());
    return cleanedTrajtroy.getTrajectoryFeatures().getLen() > minTrajLength ? cleanedTrajtroy
        : null;
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction).filter(Objects::nonNull);
  }
}
