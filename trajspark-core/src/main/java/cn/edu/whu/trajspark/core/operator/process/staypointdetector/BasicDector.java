package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
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

  private String stayPointTagName;

  /**
   * 基于密度的停留识别算法
   *
   * @param maxStayDistInMeter  停留距离阈值，单位：m
   * @param maxStayTimeInSecond 停留时间阈值，单位：s
   */
  public BasicDector(double maxStayDistInMeter, double maxStayTimeInSecond) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
    this.stayPointTagName = PreProcessDefaultConstant.DEFAULT_STAYPOINT_TAG;
  }

  public BasicDector(double maxStayDistInMeter, double maxStayTimeInSecond,
                     String stayPointTagName) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
    this.stayPointTagName = stayPointTagName;
  }

  public BasicDector(BasicDetectorConfig config) {
    this.maxStayDistInMeter = config.getMaxStayDistInMeter();
    this.maxStayTimeInSecond = config.getMaxStayTimeInSecond();
    this.stayPointTagName = config.getStayPointTagName();
  }

  @Override
  public List<StayPoint> detectFunction(Trajectory rawTrajectory) {
    Trajectory tagedTrajectory = this.detectFunctionForSegmenter(rawTrajectory);
    List<TrajPoint> ptList = tagedTrajectory.getPointList();
    if (ptList != null && !ptList.isEmpty()) {
      if (!(ptList instanceof ArrayList)) {
        ptList = new ArrayList(ptList);
      }
      List<StayPoint> spList = new ArrayList<>();

      boolean newSpFlag = true;
      int spStartIdx = Integer.MIN_VALUE;
      int spEndIdx = Integer.MIN_VALUE;
      int order = 0;
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
        if (!newSpFlag && i == spEndIdx) {
          StayPoint sp =
              new StayPoint(new ArrayList((ptList).subList(spStartIdx, spEndIdx + 1)),
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
  public Trajectory detectFunctionForSegmenter(Trajectory rawTrajectory) {
    List<TrajPoint> ptList = rawTrajectory.getPointList();
    if (ptList != null && !ptList.isEmpty()) {
      if (!(ptList instanceof ArrayList)) {
        ptList = new ArrayList<>(ptList);
      }

      List<StayPoint> spList = new ArrayList<>();
      int spEndIdx = Integer.MIN_VALUE; // 记录当前最大nextIdx，用于防止停留轨迹存在交集
      boolean newSpFlag = true;
      int spStartIdx = Integer.MIN_VALUE;

      for (int curIdx = 0; curIdx < ptList.size() - 1; ++curIdx) {
        int nextIdx = DetectUtils.findFirstExceedMaxDistIdx(ptList, curIdx,
            this.maxStayDistInMeter);
        if (spEndIdx < nextIdx
            && DetectUtils.isExceedMaxTimeThreshold(ptList, curIdx, nextIdx,
            this.maxStayTimeInSecond)) {
          // 新的停留点才改变起始位置
          if (newSpFlag) {
            spStartIdx = curIdx;
            newSpFlag = false;
          }

          spEndIdx = nextIdx;
        }
        // curIdx已经至当前停留点集的最后一个点，开始生成停留点
        if (!newSpFlag && curIdx == spEndIdx - 1) {
          // 生成停留标签
          for (int i = spStartIdx; i < curIdx + 1; ++i) {
            TrajPoint tmpP = ptList.get(i);
            tmpP.setExtendedValue(stayPointTagName, true);
            ptList.set(i, tmpP);
          }
          newSpFlag = true;
        }
      }
      return new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(), ptList,
          rawTrajectory.getExtendedValues());
    } else {
      return null;
    }
  }

  @Override
  public JavaRDD<StayPoint> detect(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> detectFunction(item).iterator());
  }

  @Override
  public String getStayPointTagName() {
    return this.stayPointTagName;
  }


}
