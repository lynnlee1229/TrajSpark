package cn.edu.whu.trajspark.core.operator.process.simplifier;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
import cn.edu.whu.trajspark.core.conf.process.simplifier.DPSimplifierConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/

/**
 * Simplify trajectory using Douglas-Peucker algorithm.
 */
public class DPSimplifier implements ISimplifier {
  private double epsilon;
  private double minTrajLength;

  public DPSimplifier(double epsilon, double minTrajLength) {
    this.epsilon = epsilon;
    this.minTrajLength = minTrajLength;
  }

  public DPSimplifier(double epsilon) {
    this.epsilon = epsilon;
    this.minTrajLength = PreProcessDefaultConstant.DEFAULT_MIN_TRAJECTORY_LEN;
  }

  public DPSimplifier(DPSimplifierConfig config) {
    this.epsilon = config.getEpsilon();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public Trajectory simplifyFunction(Trajectory rawTrajectory) {
    List<TrajPoint> tmpPointList = dpRecursion(rawTrajectory.getPointList());
    Trajectory processedTrajtroy =
        new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(),
            tmpPointList, rawTrajectory.getExtendedValues());
    return processedTrajtroy.getTrajectoryFeatures().getLen() > minTrajLength ? processedTrajtroy
        : null;
  }

  private List<TrajPoint> dpRecursion(List<TrajPoint> points) {
    // 找到最大阈值点
    double maxH = 0;
    int index = 0;
    int end = points.size();
    for (int i = 1; i < end - 1; i++) {
      double h = SimplifierUtils.getPoint2SegDis(points.get(i), points.get(0), points.get(end - 1));
      if (h > maxH) {
        maxH = h;
        index = i;
      }
    }
    // 如果存在最大阈值点，就进行递归遍历出所有最大阈值点
    List<TrajPoint> result = new ArrayList<>();
    if (maxH > epsilon) {
      List<TrajPoint> leftPoints = new ArrayList<>();// 左曲线
      List<TrajPoint> rightPoints = new ArrayList<>();// 右曲线
      // 分别提取出左曲线和右曲线的坐标点
      for (int i = 0; i < end; i++) {
        if (i <= index) {
          leftPoints.add(points.get(i));
          if (i == index) {
            rightPoints.add(points.get(i));
          }
        } else {
          rightPoints.add(points.get(i));
        }
      }

      // 分别保存两边遍历的结果
      List<TrajPoint> leftResult = new ArrayList<>();
      List<TrajPoint> rightResult = new ArrayList<>();
      leftResult = dpRecursion(leftPoints);
      rightResult = dpRecursion(rightPoints);

      // 将两边的结果整合
      rightResult.remove(0);
      leftResult.addAll(rightResult);
      result = leftResult;
    } else {
      // 如果不存在最大阈值点则返回当前遍历的子曲线的起始点
      result.add(points.get(0));
      result.add(points.get(end - 1));
    }
    return result;
  }


  @Override
  public JavaRDD<Trajectory> simplify(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::simplifyFunction);
  }
}
