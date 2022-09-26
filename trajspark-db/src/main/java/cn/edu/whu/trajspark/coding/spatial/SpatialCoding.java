package cn.edu.whu.trajspark.coding.spatial;

import cn.edu.whu.trajspark.coding.Coding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import org.locationtech.jts.geom.Polygon;

/**
 * TODO: impl
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class SpatialCoding extends Coding {
  // 对轨迹编码
  long coding(Trajectory trajectory) {
    return 0L;
  }

  /**
   * 基于编码反推索引空间几何信息
   * @param code
   * @return
   */
  Polygon getSpatialRange(long code) {
    return null;
  }

}
