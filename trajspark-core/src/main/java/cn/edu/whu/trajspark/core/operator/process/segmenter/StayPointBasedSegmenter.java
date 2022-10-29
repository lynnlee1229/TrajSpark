package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.segmenter.StayPointBasedSegmenterConfig;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.DetectUtils;
import cn.edu.whu.trajspark.core.util.GeoUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/26
 **/
public class StayPointBasedSegmenter implements ISegmenter {

  private double maxStayDistInMeter;

  private double maxStayTimeInSecond;
  private double minTrajLength;

  /**
   * 基于停留点的轨迹分段
   *
   * @param maxStayDistInMeter  停留距离阈值，单位：m
   * @param maxStayTimeInSecond 停留时间阈值，单位：s
   * @param minTrajLength       最小轨迹段长度，小于此值的轨迹段不会被生成，单位：km
   */
  public StayPointBasedSegmenter(double maxStayDistInMeter, double maxStayTimeInSecond,
                                 double minTrajLength) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 通过配置文件初始化
   *
   * @param config
   */
  public StayPointBasedSegmenter(StayPointBasedSegmenterConfig config) {
    this.maxStayDistInMeter = config.getMaxStayDistInMeter();
    this.maxStayTimeInSecond = config.getMaxStayTimeInSecond();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    List<TrajPoint> pList = rawTrajectory.getPointList();
    if (pList != null && !pList.isEmpty()) {
      if (!(pList instanceof ArrayList)) {
        pList = new ArrayList(pList);
      }

      List<Trajectory> movingTrajList = new ArrayList();
      int curIdx = 0;
      int curFurthestNextIdx = Integer.MIN_VALUE; // 记录当前最大nextIdx，用于防止停留轨迹存在交集
      boolean newStayPointFlag = true;
      int spStartIdx = Integer.MIN_VALUE;
      int trajIdx;
      int count = 0;
      Trajectory movingTraj;
      List<TrajPoint> tmpPts;
      for (trajIdx = 0; curIdx < pList.size() - 1; ++curIdx) {
        int nextIdx = DetectUtils.findFirstExceedMaxDistIdx(pList, curIdx, this.maxStayDistInMeter);
        if (curFurthestNextIdx < nextIdx &&
            DetectUtils.isExceedMaxTimeThreshold(pList, curIdx, nextIdx,
                this.maxStayTimeInSecond)) {
          if (newStayPointFlag) {
            spStartIdx = curIdx;
            newStayPointFlag = false;
          }

          curFurthestNextIdx = nextIdx;
        }
        // curIdx已经至当前停留点集的最后一个点，当前停留之前的部分至停留集合起始点被分为一个轨迹段
        if (!newStayPointFlag && curIdx == curFurthestNextIdx - 1) {
          tmpPts = new ArrayList<>(pList.subList(trajIdx, spStartIdx));
          if (GeoUtils.getTrajListLen(tmpPts) >= minTrajLength) {
            movingTraj = new Trajectory(rawTrajectory.getTrajectoryID() + "-" + count,
                rawTrajectory.getObjectID(), tmpPts, rawTrajectory.getExtendedValues());
            movingTrajList.add(movingTraj);
            count++;
          }
          // 跳过停留点，并继续发现停留点
          trajIdx = curIdx + 1;
          newStayPointFlag = true;
        }
      }
      // 上一个停留点至结尾并作一段
      // 若未发现停留点，则轨迹归为一段
      if (newStayPointFlag) {
        tmpPts = new ArrayList<>(pList.subList(trajIdx, pList.size()));
        if (GeoUtils.getTrajListLen(tmpPts) >= minTrajLength) {
          movingTraj = new Trajectory(rawTrajectory.getTrajectoryID() + "-" + count,
              rawTrajectory.getObjectID(), tmpPts, rawTrajectory.getExtendedValues());
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
