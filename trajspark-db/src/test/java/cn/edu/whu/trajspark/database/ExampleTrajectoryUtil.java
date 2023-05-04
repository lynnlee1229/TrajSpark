package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.TIME_ZONE;

/**
 * @author Haocheng Wang
 * Created on 2022/10/20
 */
public class ExampleTrajectoryUtil {

  public static List<Trajectory> parseFileToTrips(File trajFile) {
    String objectID = null;
    boolean isFirst = true;
    boolean emptyBefore = false;
    int tid = 0;
    List<Trajectory> res = new LinkedList<>();
    List<TrajPoint> trajPointList = new LinkedList<>();
    try {
      BufferedReader br = new BufferedReader(new FileReader(trajFile));
      String line;
      while ((line = br.readLine()) != null) {
        // 1,CANARQ,22.68915,114.115944,10,190,1450972811,0
        String[] tokens = line.split(",");
        objectID = tokens[1];
        int status = Integer.parseInt(tokens[7]);
        if (isFirst) {
          emptyBefore = status == 0;
          isFirst = false;
          Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
          trajPointList.add(new TrajPoint(
              ZonedDateTime.ofInstant(instant, TIME_ZONE),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        } else if (status == 1) {
          Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
          if (emptyBefore) {
            if (trajPointList.size() >= 2) {
              res.add(new Trajectory(String.valueOf(tid++), objectID, new LinkedList<>(trajPointList)));
            }
            trajPointList.clear();
            emptyBefore = false;
          }
          trajPointList.add(new TrajPoint(
              ZonedDateTime.ofInstant(instant, TIME_ZONE),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        } else {
          Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
          if (!emptyBefore) {
            if (trajPointList.size() >= 2) {
              res.add(new Trajectory(String.valueOf(tid++), objectID, new LinkedList<>(trajPointList)));
            }
            trajPointList.clear();
            emptyBefore = true;
          }
          trajPointList.add(new TrajPoint(
              ZonedDateTime.ofInstant(instant, TIME_ZONE),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        }
      }
      res.add(new Trajectory(String.valueOf(tid++), objectID, new LinkedList<>(trajPointList)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return res;
  }
}
