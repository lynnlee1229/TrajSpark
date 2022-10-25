package cn.edu.whu.trajspark.database;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.index.IndexStrategy;

import java.io.*;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/20
 */
public class ExampleTrajectoryUtil {

  public static List<Trajectory> getTrajectoriesFromResources(String dir) {
    List<Trajectory> res = new LinkedList<>();
    try {
      File f = new File(ExampleTrajectoryUtil.class.getResource(dir).toURI());
      File[] trajFiles = f.listFiles();
      for (File trajFile : trajFiles) {
        res.add(parseFile(trajFile));
      }
      System.out.println(res);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return res;
  }

  static private Trajectory parseFile(File trajFile) {
    Trajectory t = null;
    String objectID = null;
    try {
      BufferedReader br = new BufferedReader(new FileReader(trajFile));
      List<TrajPoint> trajPointList = new LinkedList<>();
      String line;
      boolean first = true;
      String trajID = null;
      while ((line = br.readLine()) != null) {
        // 1,CANARQ,22.68915,114.115944,10,190,1450972811,0
        String[] tokens = line.split(",");
        if (first) {
          objectID = tokens[1];
          trajID  = objectID + "-" + tokens[0];
          first = false;
        }
        Instant instant = Instant.ofEpochSecond(Long.parseLong(tokens[6]));
        trajPointList.add(new TrajPoint(
            ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()),
            Double.parseDouble(tokens[3]),
            Double.parseDouble(tokens[2])));
      }
      t = new Trajectory(trajID, objectID, trajPointList);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return t;
  }
}
