package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.base.util.GeoUtils;
import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/11/15
 **/
public class SegmentUtils implements Serializable {

  public static Trajectory genNewTrajectory(String tid, String oid, List<TrajPoint> pointList,
                                            Map<String, Object> extendedValue,
                                            double minTrajLength) {
    if (GeoUtils.getTrajListLen(pointList) < minTrajLength
        || pointList.size() < PreProcessDefaultConstant.DEFAULT_MIN_TRAJECTORY_NUM) {
      return null;
    } else {
      long timeHash = pointList.get(0).getTimestamp().toEpochSecond();
      return new Trajectory(tid + "_" + timeHash, oid, pointList, extendedValue);
    }
  }

}
