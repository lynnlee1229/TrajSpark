package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/26
 **/
public class BasicDector implements IDetector {
  private double maxStayDistInMeter;
  private double maxStayTimeInSecond;

  /**
   * 基于密度的停留识别算法
   *
   * @param maxStayDistInMeter  停留距离阈值，单位：m
   * @param maxStayTimeInSecond 停留时间阈值，单位：s
   */
  public BasicDector(double maxStayDistInMeter, double maxStayTimeInSecond) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
  }

  @Override
  public List<StayPoint> detectFunction(Trajectory rawTrajectory) {
    List<TrajPoint> ptList = rawTrajectory.getPointList();
    if (ptList != null && !ptList.isEmpty()) {
      if (!(ptList instanceof ArrayList)) {
        ptList = new ArrayList(ptList);
      }

      List<StayPoint> spList = new ArrayList<>();
      int curIdx = 0;
      int curFurthestNextIdx = Integer.MIN_VALUE;
      boolean newSpFlag = true;
      int spStartIdx = Integer.MIN_VALUE;

      for (int order = 0; curIdx < ptList.size() - 1; ++curIdx) {
        int nextIdx = DetectUtils.findFirstExceedMaxDistIdx(ptList, curIdx,
            this.maxStayDistInMeter);
        if (curFurthestNextIdx < nextIdx &&
            DetectUtils.isExceedMaxTimeThreshold(ptList, curIdx, nextIdx,
                this.maxStayTimeInSecond)) {
          if (newSpFlag) {
            spStartIdx = curIdx;
            newSpFlag = false;
          }

          curFurthestNextIdx = nextIdx;
        }

        if (!newSpFlag && curIdx == curFurthestNextIdx - 1) {
          StayPoint sp =
              new StayPoint(new ArrayList((ptList).subList(spStartIdx, curIdx + 1)),
                  String.format("%s_%d", rawTrajectory.getTrajectoryID(), order++),
                  rawTrajectory.getObjectID());
          spList.add(sp);
          newSpFlag = true;
        }
      }

      return spList;
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public JavaRDD<StayPoint> detect(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> detectFunction(item).iterator());
  }
}
