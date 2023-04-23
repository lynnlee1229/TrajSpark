package cn.edu.whu.trajspark.query.advanced;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.base.util.GeoUtils;

/**
 * @author Haocheng Wang
 * Created on 2023/2/16
 */
public class KNNTrajectory {

  private double distanceToPoint = -1.0;
  private Trajectory trajectory;

  public KNNTrajectory(Trajectory trajectory) {
    this.trajectory = trajectory;
  }

  public KNNTrajectory(Trajectory trajectory, BasePoint queryPoint) {
    this.trajectory = trajectory;
    calDistanceToPoint(queryPoint);
  }

  public double getDistanceToPoint() {
    return distanceToPoint;
  }

  public Trajectory getTrajectory() {
    return trajectory;
  }

  private void calDistanceToPoint(BasePoint p) {
    double distance = Double.MAX_VALUE;
    for (TrajPoint trajPoint : this.trajectory.getPointList()) {
      BasePoint trajBasePoint = new BasePoint(trajPoint.getLng(), trajPoint.getLat());
      double pDistance = GeoUtils.getEuclideanDistanceM(p, trajBasePoint);
      if (pDistance < distance) {
        distance = pDistance;
      }
    }
    this.distanceToPoint = distance;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof KNNTrajectory) {
      KNNTrajectory trajectory = (KNNTrajectory) obj;
      if (!trajectory.equals(this.trajectory)) {
        return false;
      }
      if (Math.abs(distanceToPoint - trajectory.distanceToPoint) > 1e-8) {
        return false;
      }
      return true;
    }
    return false;
  }



}
