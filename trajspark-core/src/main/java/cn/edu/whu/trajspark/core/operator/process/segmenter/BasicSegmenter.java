package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.segmenter.BasicSegmenterConfig;
import cn.edu.whu.trajspark.base.util.GeoUtils;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/3
 **/
public class BasicSegmenter implements ISegmenter {
  private double maxDis;
  private double maxTimeInterval;
  private double minTrajLength;

  /**
   * 分段参数
   *
   * @param maxDis          分段距离阈值，单位：m
   * @param maxTimeInterval 分段时间阈值，单位：s
   * @param minTrajLength   分段后最小轨迹长度，小于该长度则去除，单位：km
   */
  public BasicSegmenter(double maxDis, double maxTimeInterval, double minTrajLength) {
    this.maxDis = maxDis;
    this.maxTimeInterval = maxTimeInterval;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 通过配置文件初始化
   * @param config
   */
  public BasicSegmenter(BasicSegmenterConfig config) {
    this.maxDis = config.getMaxDis();
    this.maxTimeInterval = config.getMaxTimeInterval();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    List<TrajPoint> tmpPointList = rawTrajectory.getPointList();
    List<Trajectory> res = new ArrayList<>();
    List<Integer> segIndex = new ArrayList<>();
    segIndex.add(-1);
    for (int i = 0; i < tmpPointList.size() - 1; ++i) {
      TrajPoint p0 = tmpPointList.get(i), p1 = tmpPointList.get(i + 1);
      if (ChronoUnit.SECONDS.between(p0.getTimestamp(), p1.getTimestamp()) >= maxTimeInterval ||
          GeoUtils.getEuclideanDistanceM(p0, p1) >= maxDis) {
        // record segIndex
        segIndex.add(i);
      }
    }
    segIndex.add(tmpPointList.size() - 1);
    // do segment
    int n = segIndex.size();
    int count = 0;
    if (n == 2) {
      rawTrajectory.setTrajectoryID(rawTrajectory.getTrajectoryID() + "-" + count);
      res.add(rawTrajectory);
    } else {
      for (int i = 0; i < n - 1; ++i) {
        List<TrajPoint> tmpPts =
            new ArrayList<>(tmpPointList.subList(segIndex.get(i) + 1, segIndex.get(i + 1)));
        if (GeoUtils.getTrajListLen(tmpPts) < minTrajLength) {
          continue;
        }
        Trajectory tmp = new Trajectory(rawTrajectory.getTrajectoryID() + "-" + count,
            rawTrajectory.getObjectID(), tmpPts, rawTrajectory.getExtendedValues());
        res.add(tmp);
        count++;
      }
    }
    return res;
  }

  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator());
  }
}
