package cn.edu.whu.trajspark.core.operator.store.convertor.basic;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/11/6
 **/
public class StayPointConvertor implements Serializable {
  public static String convertSP(StayPoint stayPoint) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String record = stayPoint.getSid() + "," + stayPoint.getOid() + ","
        + stayPoint.getStartTime().format(formatter) + ","
        + stayPoint.getEndTime().format(formatter) + ","
        + stayPoint.getCenterPoint().getLat() + ","
        + stayPoint.getCenterPoint().getLng();
    return record;
  }

  public static String convertSPList(List<StayPoint> spList) {
    StringBuilder records = new StringBuilder();
    for (StayPoint stayPoint : spList) {
      records.append(convertSP(stayPoint)).append('\n');
    }
    return records.toString();
  }

  public static String convertSPAsTraj(StayPoint stayPoint) {
    return TrajectoryConvertor.convert(
        new Trajectory(stayPoint.getSid(), stayPoint.getOid(), stayPoint.getPlist()));
  }

}
