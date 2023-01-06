package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.segmenter.StayPointBasedSegmenterConfig;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.IDetector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/26
 **/
public class StayPointBasedSegmenter implements ISegmenter {

  // TODO 持有检测算子
  private IDetector detector;

  private double minTrajLength;

  /**
   * 基于停留点的轨迹分段
   * 若已有停留标签，则直接分段，否则首先进行停留检测
   *
   * @param detector      停留检测器
   * @param minTrajLength 最小轨迹长度，单位：km
   */
  public StayPointBasedSegmenter(IDetector detector, double minTrajLength) {
    this.detector = detector;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 通过配置文件初始化
   *
   * @param config
   */
  public StayPointBasedSegmenter(StayPointBasedSegmenterConfig config) {
    this.minTrajLength = config.getMinTrajLength();
    this.detector = IDetector.getDector(config.getDetectorConfig());
  }

  public void setDetector(IDetector detector) {
    this.detector = detector;
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    Trajectory tagedTrajectory = detector.detectFunctionForSegmenter(rawTrajectory);
    List<TrajPoint> ptList = tagedTrajectory.getPointList();
    if (ptList != null && !ptList.isEmpty()) {
      if (!(ptList instanceof ArrayList)) {
        ptList = new ArrayList<>(ptList);
      }
      List<Trajectory> movingTrajList = new ArrayList<>();
      Trajectory movingTraj;
      List<TrajPoint> tmpPts;
      boolean newSpFlag = true;
      int spStartIdx = Integer.MIN_VALUE;
      int spEndIdx = Integer.MIN_VALUE;
      int trajIdx = 0;
      String stayPointTagName = detector.getStayPointTagName();
      for (int i = 0; i < ptList.size() - 1; ++i) {
        int j = i;
        while (ptList.get(j).getExtendedValue(stayPointTagName) != null
            && (boolean) ptList.get(j).getExtendedValue(stayPointTagName)) {
          // 记录停留点起点
          if (newSpFlag) {
            spStartIdx = j;
            newSpFlag = false;
          }
          j++;
        }
        if (j != i) {
          spEndIdx = j - 1;
        }
        // 根据Tag生成停留点
        // 从trajIdx到最近停留点的前一点
        if (!newSpFlag && i == spEndIdx) {
          tmpPts = new ArrayList<>(ptList.subList(trajIdx, spStartIdx));
          movingTraj = SegmentUtils.genNewTrajectory(
              rawTrajectory.getTrajectoryID(),
              rawTrajectory.getObjectID(),
              tmpPts,
              rawTrajectory.getExtendedValues(),
              minTrajLength);
          if (movingTraj != null) {
            movingTrajList.add(movingTraj);
          }
          // 跳过停留点，并继续发现停留点
          trajIdx = spEndIdx + 1;
          newSpFlag = true;
        }
      }
      // 上一个停留点至结尾并作一段
      // 若未发现停留点，则轨迹归为一段
      if (newSpFlag) {
        tmpPts = new ArrayList<>(ptList.subList(trajIdx, ptList.size()));
        movingTraj = SegmentUtils.genNewTrajectory(
            rawTrajectory.getTrajectoryID(),
            rawTrajectory.getObjectID(),
            tmpPts,
            rawTrajectory.getExtendedValues(),
            minTrajLength);
        if (movingTraj != null) {
          movingTrajList.add(movingTraj);
        }
      }
      return movingTrajList;
    } else {
      return Collections.emptyList();
    }

  }


  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator());
  }
}
