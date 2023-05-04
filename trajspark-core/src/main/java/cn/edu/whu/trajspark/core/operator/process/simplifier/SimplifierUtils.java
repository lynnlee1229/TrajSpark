package cn.edu.whu.trajspark.core.operator.process.simplifier;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.util.GeoUtils;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
public class SimplifierUtils {
  /**
   * 计算点到线段的距离
   * @param pt 点
   * @param start 起点
   * @param end 终点
   * @return 距离
   */
  public static double getPoint2SegDis(TrajPoint pt, TrajPoint start, TrajPoint end) {
    double s1 = GeoUtils.getEuclideanDistanceM(start, end);
    double s2 = GeoUtils.getEuclideanDistanceM(pt, start);
    double s3 = GeoUtils.getEuclideanDistanceM(pt, end);
    double p = (s2 + s3 + s1) / 2;
    double s = Math.sqrt(p * (p - s2) * (p - s3) * (p - s1));
    return 2 * s / s1;
  }

}
