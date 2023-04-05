package cn.edu.whu.trajspark.core.operator.analysis.geofence;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
import cn.edu.whu.trajspark.core.enums.TopologyTypeEnum;
import cn.edu.whu.trajspark.core.util.EvaluatorUtils;
import cn.edu.whu.trajspark.core.util.EvaluatorUtils.SpatialPredicateEvaluator;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import scala.Serializable;
import scala.Tuple2;

/**
 * @author Lynn Lee
 * @date 2023/3/30
 **/
public class Geofence<T extends Geometry> implements Serializable {
  private SpatialPredicateEvaluator evaluator;

  public Geofence() {
    evaluator = EvaluatorUtils.create(TopologyTypeEnum.INTERSECTS);
  }

  public Tuple2<String, String> geofence(Trajectory traj, TreeIndex<T> treeIndex) {
    LineString trajLine = traj.getLineString();
    Tuple2<String, String> res = null;
    List<TrajPoint> rawPointList = traj.getPointList();
    for (TrajPoint point : rawPointList) {
      List<T> result = treeIndex.query(trajLine);
      if (result.size() == 0) {
        return null;
      }
      for (T polygon : result) {
        if (evaluator.eval(polygon, trajLine)) {
          return new Tuple2<>(traj.getObjectID() + "-" + traj.getTrajectoryID(),
              (String) polygon.getUserData());
        }
      }
    }
    return null;
  }
}
