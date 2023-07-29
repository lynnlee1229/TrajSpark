package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.FilterUtils;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.SegmentUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.spark.api.java.JavaRDD;

public class SimuSegmenter implements ISegmenter {

  private final int simuTimes;
  private final int plusTimes;

  public SimuSegmenter(int simuTimes, int plusTimes) {
    this.simuTimes = simuTimes;
    this.plusTimes = plusTimes;
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    List<Trajectory> res = new ArrayList<>();;
    for (int i = 0; i < simuTimes; i++) {
      List<TrajPoint> tmpPointList = FilterUtils.sortPointList(rawTrajectory.getPointList());
      for (int j = 0; j < tmpPointList.size(); j++) {
        TrajPoint tmpP = tmpPointList.get(j);
        tmpPointList.get(j).setTimestamp(tmpP.getTimestamp().plusWeeks(plusTimes));
      }
      Trajectory tmp = SegmentUtils.genNewTrajectory(
          simuTimes + "_" + rawTrajectory.getTrajectoryID(),
          rawTrajectory.getObjectID(),
          tmpPointList,
          rawTrajectory.getExtendedValues(),
          PreProcessDefaultConstant.DEFAULT_MIN_TRAJECTORY_LEN);
      res.add(tmp);
    }
    return res;
  }

  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator())
        .filter(Objects::nonNull);
  }
}
