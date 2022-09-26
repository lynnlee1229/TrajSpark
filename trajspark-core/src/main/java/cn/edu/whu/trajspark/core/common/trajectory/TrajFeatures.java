package cn.edu.whu.trajspark.core.common.trajectory;

import cn.edu.whu.trajspark.core.common.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import java.io.Serializable;
import java.time.ZonedDateTime;

/**
 * @author Lynn Lee
 * @date 2022/9/7
 **/
public class TrajFeatures implements Serializable {
  private ZonedDateTime startTime;
  private ZonedDateTime endTime;
  private TrajPoint startPoint;
  private TrajPoint endPoint;
  private int pointNum;
  private MinimumBoundingBox mbr;
  private double speed;
  private double len;

  public TrajFeatures(ZonedDateTime startTime, ZonedDateTime endTime, TrajPoint startPoint,
                      TrajPoint endPoint, int pointNum, MinimumBoundingBox mbr, double speed,
                      double len) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.startPoint = startPoint;
    this.endPoint = endPoint;
    this.pointNum = pointNum;
    this.mbr = mbr;
    this.speed = speed;
    this.len = len;
  }

  public ZonedDateTime getStartTime() {
    return startTime;
  }

  public ZonedDateTime getEndTime() {
    return endTime;
  }

  public TrajPoint getStartPoint() {
    return startPoint;
  }

  public TrajPoint getEndPoint() {
    return endPoint;
  }

  public int getPointNum() {
    return pointNum;
  }

  public MinimumBoundingBox getMbr() {
    return mbr;
  }

  public double getSpeed() {
    return speed;
  }

  public double getLen() {
    return len;
  }
}
