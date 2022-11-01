package cn.edu.whu.trajspark.example.database;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/11/1
 */
public class ExampleDataUtils {


  public static List<Trajectory> parseFileToTrips() {
    String objectID = null;
    boolean isFirst = true;
    boolean emptyBefore = false;
    List<Trajectory> res = new LinkedList<>();
    List<TrajPoint> trajPointList = new LinkedList<>();
    try {
      File f = new File(ExampleDataUtils.class.getResource("/data/database/CBQBDS").toURI());
      BufferedReader br = new BufferedReader(new FileReader(f));
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
              ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        } else if (status == 1) {
          Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
          if (emptyBefore) {
            if (trajPointList.size() >= 2) {
              res.add(new Trajectory("tid", objectID, new LinkedList<>(trajPointList)));
            }
            trajPointList.clear();
            emptyBefore = false;
          }
          trajPointList.add(new TrajPoint(
              ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        } else {
          Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
          if (!emptyBefore) {
            if (trajPointList.size() >= 2) {
              res.add(new Trajectory("tid", objectID, new LinkedList<>(trajPointList)));
            }
            trajPointList.clear();
            emptyBefore = true;
          }
          trajPointList.add(new TrajPoint(
              ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()),
              Double.parseDouble(tokens[3]),
              Double.parseDouble(tokens[2])));
        }
      }
      res.add(new Trajectory(String.valueOf(System.currentTimeMillis()), objectID, new LinkedList<>(trajPointList)));
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return res;
  }



}
