package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.util.GeoUtils;
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
  private double minTrajLength;
  private double maxTimeInterval;


  public BasicSegmenter(double maxDis, double minTrajLength, double maxTimeInterval) {
    this.maxDis = maxDis;
    this.minTrajLength = minTrajLength;
    this.maxTimeInterval = maxTimeInterval;
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
          GeoUtils.getEuclideanDistance(p0, p1) >= maxDis) {
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
