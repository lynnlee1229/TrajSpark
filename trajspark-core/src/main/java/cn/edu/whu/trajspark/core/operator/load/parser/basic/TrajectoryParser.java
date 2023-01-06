package cn.edu.whu.trajspark.core.operator.load.parser.basic;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.data.TrajectoryConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Lynn Lee
 * @date 2022/11/2
 **/
public class TrajectoryParser {
  public static Trajectory multifileParse(String rawString,
                                          TrajectoryConfig trajectoryConfig,
                                          String splitter) throws IOException {
    String[] points = rawString.split(System.lineSeparator());
    int n = points.length;
    List<TrajPoint> trajPoints = new ArrayList<>(n);
    String trajId = "";
    String objectId = "";
    String pStr;
    boolean genPid = false;
    for (int i = 0; i < n; ++i) {
      pStr = points[i];
      if (i == 0) {
        String[] firstP = pStr.split(splitter);
        int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
        int trajIdIndex = trajectoryConfig.getTrajId().getIndex();
        if (trajIdIndex >= 0) {
          trajId = firstP[trajIdIndex];
        }
        if (objectIdIndex >= 0) {
          objectId = firstP[objectIdIndex];
        }
      }
      TrajPoint point =
          TrajPointParser.parse(pStr, trajectoryConfig.getTrajPointConfig(),
              splitter);
      if (point.getPid() == null) {
        genPid = true;
      }
      trajPoints.add(point);
    }
    return trajPoints.isEmpty() ? null : new Trajectory(trajId, objectId, trajPoints, genPid);
  }

  public static List<Trajectory> singlefileParse(String rawString,
                                                 TrajectoryConfig trajectoryConfig,
                                                 String splitter) throws IOException {
    int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
    int trajIdIndex = trajectoryConfig.getTrajId().getIndex();
    String[] points = rawString.split(System.lineSeparator());
    // 按tid+oid分组
    Map<String, List<String>> groupList = Arrays.stream(points).collect(
        Collectors.groupingBy(item -> getGroupKey(item, splitter, trajIdIndex, objectIdIndex)));
    // 映射
    return groupList.entrySet().stream()
        .map(Map.Entry::getValue)
        .map(item -> {
          try {
            return mapToTraj(item, splitter, trajIdIndex, objectIdIndex, trajectoryConfig);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static String getGroupKey(String line, String splitter, int trajIdIndex,
                                    int objectIdIndex) {
    String[] tmpP = line.split(splitter);
    return tmpP[trajIdIndex] + "#" + tmpP[objectIdIndex];
  }

  private static Trajectory mapToTraj(List<String> points, String splitter, int trajIdIndex,
                                      int objectIdIndex, TrajectoryConfig trajectoryConfig)
      throws IOException {
    String trajId = "", objectId = "";
    List<TrajPoint> trajPoints = new ArrayList<>(points.size());
    for (String point : points) {
      String[] tmpP = point.split(splitter);
      trajId = tmpP[trajIdIndex];
      objectId = tmpP[objectIdIndex];
      TrajPoint trajPoint =
          TrajPointParser.parse(point, trajectoryConfig.getTrajPointConfig(),
              splitter);
      trajPoints.add(trajPoint);
    }
    return trajPoints.isEmpty() ? null : new Trajectory(trajId, objectId, trajPoints);

  }
}
