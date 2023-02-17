package cn.edu.whu.trajspark.core.operator.process.interpolation;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.constant.DateDefaultConstant;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
public class LinearInterpolation {
  private static final int MINIMUMSIZE = 3;
  private static final int MAXMUMTIMEINTERVAL = 60;
  private int intervalTime;

  private Timestamp time2Timestamp(ZonedDateTime time) {
    return Timestamp.from(time.toInstant());
  }

  private TrajPoint getLinearAddedPt(TrajPoint preP, TrajPoint nextP, Timestamp time) {
    ZonedDateTime ztime =
        ZonedDateTime.ofInstant(time.toInstant(), DateDefaultConstant.DEFAULT_ZONE_ID);
    TrajPoint result = new TrajPoint(ztime, 0, 0);
    double tSum = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), nextP.getTimestamp()));
    double t = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), ztime));
    if (t != 0) {
      result.setLng(preP.getLng() + (nextP.getLng() - preP.getLng()) * t / tSum);
      result.setLat(preP.getLat() + (nextP.getLat() - preP.getLat()) * t / tSum);

    } else {
      result.setLng(preP.getLng());
      result.setLat(preP.getLat());
    }
    return result;
  }


  public Trajectory interpolation(Trajectory rawTrajectory) {

    if (rawTrajectory.getPointList().size() < MINIMUMSIZE) {
      throw new IllegalArgumentException("Trajectory must have at least 3 points");
    } else if (intervalTime > MAXMUMTIMEINTERVAL) {
      throw new IllegalArgumentException("Interval time is larger than 1 min");
    } else {
      List<TrajPoint> ptList = rawTrajectory.getPointList();
      List<TrajPoint> interpolatedList = new ArrayList();
      interpolatedList.add(ptList.get(0));
      Timestamp timePointer = Timestamp.from(
          (ptList.get(0)).getTimestamp().toInstant().plusSeconds((long) intervalTime));
      for (int idxPointer = 0;
           !timePointer.after(time2Timestamp((ptList.get(ptList.size() - 1)).getTimestamp()));
           timePointer = Timestamp.from(timePointer.toInstant().plusSeconds((long) intervalTime))) {
        for (int idx = idxPointer; idx < ptList.size(); ++idx) {
          if ((time2Timestamp(ptList.get(idx).getTimestamp())).equals(timePointer)) {
            idxPointer = idx;
            interpolatedList.add(ptList.get(idx));
            break;
          }

          if (time2Timestamp(ptList.get(idx).getTimestamp()).after(timePointer)) {
            idxPointer = idx;
            TrajPoint prePt = ptList.get(idx - 1);
            TrajPoint afterPt = ptList.get(idx);
            TrajPoint addedPt = getLinearAddedPt(prePt, afterPt, timePointer);
            interpolatedList.add(addedPt);
            break;
          }
        }
      }
      return new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(),
          interpolatedList, rawTrajectory.getExtendedValues());
    }
  }


}
