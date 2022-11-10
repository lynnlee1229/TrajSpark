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
  public static String convertSP(StayPoint stayPoint,
                                 String splitter) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String record =
        stayPoint.getSid() + splitter
            + stayPoint.getOid() + splitter
            + stayPoint.getStartTime().format(formatter) + splitter
            + stayPoint.getEndTime().format(formatter) + splitter
            + stayPoint.getCenterPoint().getLat() + splitter
            + stayPoint.getCenterPoint().getLng();
    return record;
  }

  public static String convertSPList(List<StayPoint> spList,
                                     String splitter,
                                     String lineBreaker) {
    StringBuilder records = new StringBuilder();
    for (StayPoint stayPoint : spList) {
      records.append(convertSP(stayPoint, splitter)).append(lineBreaker);
    }
    return records.toString();
  }

  public static String convertSPAsTraj(StayPoint stayPoint,
                                       String splitter,
                                       String lineBreaker) {
    return TrajectoryConvertor.convert(
        new Trajectory(stayPoint.getSid(), stayPoint.getOid(), stayPoint.getPlist()), splitter,
        lineBreaker);
  }

}
