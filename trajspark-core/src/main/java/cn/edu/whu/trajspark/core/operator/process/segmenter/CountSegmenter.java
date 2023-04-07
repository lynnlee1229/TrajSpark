package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.FilterUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2023/4/7
 **/
public class CountSegmenter implements ISegmenter {

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    Random random = new Random();
    List<TrajPoint> tmpPointList = FilterUtils.sortPointList(rawTrajectory.getPointList());
    List<Trajectory> res = new ArrayList<>();
    List<Integer> segIndex = new ArrayList<>();
    segIndex.add(0);
    int threshold = random.nextInt(20) + 30;
    int count = 0;
    for (int i = 0; i < tmpPointList.size() - 1; ++i) {
      count++;
      if (count >= threshold) {
        segIndex.add(i + 1);
        count = 0;
        threshold = random.nextInt(20) + 30;
      }
    }
    segIndex.add(tmpPointList.size());
    // do segment
    int n = segIndex.size();
    for (int i = 0; i < n - 1; ++i) {
      List<TrajPoint> tmpPts =
          new ArrayList<>(tmpPointList.subList(segIndex.get(i), segIndex.get(i + 1)));
      Trajectory tmp = SegmentUtils.genNewTrajectory(
          rawTrajectory.getTrajectoryID(),
          rawTrajectory.getObjectID(),
          tmpPts,
          rawTrajectory.getExtendedValues(),
          0);
      if (tmp == null) {
        continue;
      }
      res.add(tmp);
    }
    return res;
  }

  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator());
  }
}
