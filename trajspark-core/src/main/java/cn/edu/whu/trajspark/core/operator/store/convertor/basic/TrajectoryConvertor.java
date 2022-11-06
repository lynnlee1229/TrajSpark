package cn.edu.whu.trajspark.core.operator.store.convertor.basic;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/11/2
 **/
public class TrajectoryConvertor implements Serializable {
  public static String convert(Trajectory trajectory) {
    StringBuilder records = new StringBuilder();
    String tid = trajectory.getTrajectoryID();
    String oid = trajectory.getObjectID();
    List<TrajPoint> pointList = trajectory.getPointList();
    String splitter = ",";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    for (TrajPoint tmpP : pointList) {
      StringBuilder record = new StringBuilder();
      record.append(tid).append(splitter);
      record.append(oid).append(splitter);
      record.append(tmpP.getPid()).append(splitter);
      record.append(tmpP.getLat()).append(splitter);
      record.append(tmpP.getLng()).append(splitter);
      record.append(tmpP.getTimestamp().format(formatter));
      if (null != tmpP.getExtendedValues()) {
        Iterator pointIter =
            tmpP.getExtendedValues().entrySet().iterator();

        while (pointIter.hasNext()) {
          Map.Entry<String, Object> set = (Map.Entry) pointIter.next();
          record.append(splitter).append(set.getValue());
        }
      }
      records.append(record).append('\n');
    }
    return records.toString();
  }
}
