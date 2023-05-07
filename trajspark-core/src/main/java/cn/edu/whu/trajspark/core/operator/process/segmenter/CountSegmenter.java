package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
import cn.edu.whu.trajspark.core.conf.process.segmenter.CountSegmenterConfig;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.FilterUtils;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2023/4/7
 **/
public class CountSegmenter implements ISegmenter {
  private final int minSegNum;
  private final int maxSegNum;
  private final double maxTimeInterval;
  private final Random random;

  public CountSegmenter(long seed, int minSegNum, int maxSegNum, double maxTimeInterval) {
    this.minSegNum = minSegNum;
    this.maxSegNum = maxSegNum;
    this.maxTimeInterval = maxTimeInterval;
    random = new Random(seed);
  }

  public CountSegmenter(int minSegNum, int maxSegNum, double maxTimeInterval) {
    this.minSegNum = minSegNum;
    this.maxSegNum = maxSegNum;
    this.maxTimeInterval = maxTimeInterval;
    random = new Random();
  }

  public CountSegmenter(CountSegmenterConfig config) {
    this.minSegNum = config.getMinSegNum();
    this.maxSegNum = config.getMaxSegNum();
    this.maxTimeInterval = config.getMaxTimeInterval();
    if (config.getSeed() != null) {
      random = new Random(config.getSeed());
    } else {
      random = new Random();
    }
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    // sort by timestamp
    List<TrajPoint> tmpPointList = FilterUtils.sortPointList(rawTrajectory.getPointList());
    List<Trajectory> res = new ArrayList<>();
    List<Integer> segIndex = new ArrayList<>();
    segIndex.add(0);
    int threshold = random.nextInt((maxSegNum - minSegNum)) + minSegNum;
    int count = 0;
    for (int i = 0; i < tmpPointList.size() - 1; ++i) {
      TrajPoint p0 = tmpPointList.get(i), p1 = tmpPointList.get(i + 1);
      count++;
      if (ChronoUnit.SECONDS.between(p0.getTimestamp(), p1.getTimestamp()) >= maxTimeInterval
          || count >= threshold) {
        segIndex.add(i + 1);
        count = 0;
        threshold = random.nextInt((maxSegNum - minSegNum)) + minSegNum;
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
          PreProcessDefaultConstant.DEFAULT_MIN_TRAJECTORY_LEN);
      if (tmp == null) {
        continue;
      }
      res.add(tmp);
    }
    return res;
  }

  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator()).filter(Objects::nonNull);
  }
}
