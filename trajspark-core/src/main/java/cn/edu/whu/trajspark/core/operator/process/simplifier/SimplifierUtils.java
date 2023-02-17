package cn.edu.whu.trajspark.core.operator.process.simplifier;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.util.GeoUtils;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
public class SimplifierUtils {
  /**
   * 计算点到直线的距离
   *
   * @param p
   * @param s
   * @param e
   * @return
   */
  public static double getPoint2SegDis(TrajPoint pt, TrajPoint start, TrajPoint end) {
    double s1 = GeoUtils.getEuclideanDistance(start, end);
    double s2 = GeoUtils.getEuclideanDistance(pt, start);
    double s3 = GeoUtils.getEuclideanDistance(pt, end);
    double p = (s2 + s3 + s1) / 2;
    double s = Math.sqrt(p * (p - s2) * (p - s3) * (p - s1));
    double dis = 2 * s / s1;
    return dis;
  }

}
