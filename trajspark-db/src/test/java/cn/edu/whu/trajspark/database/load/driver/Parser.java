package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/27
 */
public class Parser extends TextTrajParser {
  @Override
  public Trajectory parse(String line) throws ParseException {
    String[] strs = line.split("\\|");
    String carNo = strs[0];
    Trajectory t = new Trajectory(
        getTrajectoryID(line),
        carNo,
        toTrajPointList(strs[2], strs[1]));
    return t;
  }

  private String getTrajectoryID(String line) {
    String[] timestampStrs = line.split("\\|")[1].split(",");
    return timestampStrs[0];
  }

  private List<TrajPoint> toTrajPointList(String lineWKT, String timestampStr) throws ParseException {
    List<TrajPoint> list = new LinkedList<>();
    String[] timestampStrs = timestampStr.split(",");
    WKTReader wktReader = new WKTReader();
    Coordinate[] coordinates = ((LineString) wktReader.read(lineWKT)).getCoordinates();
    for (int i = 0; i < coordinates.length; i++) {
      Instant instant = Instant.ofEpochSecond(Long.parseLong(timestampStrs[i]));
      ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
      double lng = coordinates[i].y;
      double lat = coordinates[i].x;
      TrajPoint trajPoint = new TrajPoint(zonedDateTime, lng, lat);
      list.add(trajPoint);
    }
    return list;
  }
}