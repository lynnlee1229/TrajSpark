package cn.edu.whu.trajspark.core.operator.process.staypointdetector;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.util.GeoUtils;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/10/26
 **/
public class DetectUtils {
  public static int findFirstExceedMaxDistIdx(List plist, int idx, double maxStayDistInMeter) {
    TrajPoint curP = (TrajPoint) plist.get(idx);
    ++idx;

    while (idx < plist.size()) {
      TrajPoint tmpP = (TrajPoint) plist.get(idx);
      if (GeoUtils.getEuclideanDistanceM(curP, tmpP) > maxStayDistInMeter) {
        break;
      }

      ++idx;
    }

    return idx;
  }

  public static boolean isExceedMaxTimeThreshold(List plist, int curIdx, int nextIdx, double maxStayTimeInSecond) {
    return (double) ChronoUnit.SECONDS.between(((TrajPoint) plist.get(curIdx)).getTimestamp(),
        ((TrajPoint) plist.get(nextIdx - 1)).getTimestamp()) > maxStayTimeInSecond;
  }
}
