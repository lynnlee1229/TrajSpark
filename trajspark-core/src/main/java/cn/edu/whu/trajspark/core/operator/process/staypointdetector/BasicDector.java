package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.detector.BasicDetectorConfig;
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

  public BasicDector(BasicDetectorConfig config) {
    this.maxStayDistInMeter = config.getMaxStayDistInMeter();
    this.maxStayTimeInSecond = config.getMaxStayTimeInSecond();
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
      int curFurthestNextIdx = Integer.MIN_VALUE; // 记录当前最大nextIdx，用于防止停留轨迹存在交集
      boolean newSpFlag = true;
      int spStartIdx = Integer.MIN_VALUE;

      for (int order = 0; curIdx < ptList.size() - 1; ++curIdx) {
        int nextIdx = DetectUtils.findFirstExceedMaxDistIdx(ptList, curIdx,
            this.maxStayDistInMeter);
        if (curFurthestNextIdx < nextIdx
            && DetectUtils.isExceedMaxTimeThreshold(ptList, curIdx, nextIdx,
            this.maxStayTimeInSecond)) {
          // 新的停留点才改变起始位置
          if (newSpFlag) {
            spStartIdx = curIdx;
            newSpFlag = false;
          }

          curFurthestNextIdx = nextIdx;
        }
        // curIdx已经至当前停留点集的最后一个点，开始生成停留点
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
