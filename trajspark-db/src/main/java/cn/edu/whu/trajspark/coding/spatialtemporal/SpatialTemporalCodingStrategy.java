package cn.edu.whu.trajspark.coding.spatialtemporal;

import cn.edu.whu.trajspark.coding.CodingStrategy;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import org.locationtech.geomesa.curve.BinnedTime;
import org.locationtech.jts.geom.Polygon;

/**
 * TODO:
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class SpatialTemporalCodingStrategy extends CodingStrategy {
  // 对轨迹编码
  long coding(Trajectory trajectory) {
    return 0L;
  }

  /**
   * 基于编码反推索引中的空间几何信息
   * @param code
   * @return
   */
  Polygon getSpatialRange(long code) {
    return null;
  }

  /**
   * 基于编码反推索引中的时间信息
   * @param code
   * @return
   */
  BinnedTime getBinnedTime(long code) {
    return null;
  }
}
