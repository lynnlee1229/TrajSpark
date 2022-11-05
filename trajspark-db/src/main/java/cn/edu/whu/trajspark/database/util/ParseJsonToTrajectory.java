package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.coding.utils.DateTimeParse;
import cn.edu.whu.trajspark.core.common.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
public class ParseJsonToTrajectory {

  /**
   * Turning GeoJson data into a memory Trajectory object
   * @param value json object
   * @return Trajectory trajectory
   */
  public static Trajectory parseJsonToTrajectory(String value) {
    JSONObject feature = JSONObject.parseObject(value);
    JSONObject properties = feature.getJSONObject("properties");
    String tid = properties.getString("tid");
    String oid = properties.getString("oid");
    TrajFeatures trajFeatures = parseTraFeatures(properties);
    JSONObject geometry = feature.getJSONObject("geometry");
    JSONArray coordinates = geometry.getJSONArray("coordinates");
    List<TrajPoint> traPoints = parseTraPointList(coordinates, properties);
    return new Trajectory(tid, oid, traPoints, trajFeatures);
  }

  public static TrajFeatures parseTraFeatures(JSONObject properties) {
    String oid = properties.getString("oid");
    String tid = properties.getString("tid");
    JSONArray mbr = properties.getJSONArray("mbr");
    MinimumBoundingBox box = parseMBR(mbr);

    Long sTime = properties.getLong("start_time");
    ZonedDateTime startTime = DateTimeParse.timeToZonedTime(sTime);
    Long eTime = properties.getLong("end_time");
    ZonedDateTime endTime = DateTimeParse.timeToZonedTime(eTime);

    JSONArray startPoint = properties.getJSONArray("start_point");
    TrajPoint traStartPoint = parsePoint(startPoint, properties, true);
    JSONArray endPoint = properties.getJSONArray("end_point");
    TrajPoint traEndPoint = parsePoint(endPoint, properties, false);

    Integer pointNumber = properties.getInteger("pointNumber");
    JSONArray speedArray = properties.getJSONArray("speed");
    Double traSpeed = parseTraSpeed(speedArray);

    Double length = properties.getDouble("length");
    return new TrajFeatures(startTime, endTime, traStartPoint,
        traEndPoint, pointNumber, box, traSpeed, length);
  }

  public static TrajPoint parsePoint(JSONArray point, JSONObject properties, Boolean isSTPoint) {
    JSONArray timestamp = properties.getJSONArray("timestamp");
    int pid;
    Long sTime;
    Double speed;
    HashMap<String, Object> stringObjectMap = new HashMap<>();
    JSONArray speedArray = properties.getJSONArray("speed");
    if (isSTPoint) {
      sTime = timestamp.getLong(0);
      speed = speedArray.getDouble(0);
      pid = 0;
    } else {
      sTime = timestamp.getLong(timestamp.size() - 1);
      speed = speedArray.getDouble(timestamp.size() - 1);
      pid = timestamp.size() - 1;
    }
    stringObjectMap.put("Time", sTime);
    stringObjectMap.put("Speed", speed);

    return new TrajPoint(
        Integer.toString(pid),
        DateTimeParse.timeToZonedTime(sTime),
        point.getDouble(0),
        point.getDouble(1),
        stringObjectMap);
  }

  public static MinimumBoundingBox parseMBR(JSONArray mbr) {
    Double lng1 = mbr.getJSONArray(0).getDouble(0);
    Double lat1 = mbr.getJSONArray(0).getDouble(1);
    Double lng2 = mbr.getJSONArray(1).getDouble(0);
    Double lat2 = mbr.getJSONArray(1).getDouble(1);
    return new MinimumBoundingBox(lng1, lat1, lng2, lat2);
  }

  public static Double parseTraSpeed(JSONArray speed) {
    Double traSpeed = 0.0;
    for (int i = 0; i < speed.size(); i++) {
      traSpeed += speed.getDouble(i);
    }
    traSpeed = traSpeed / speed.size();
    return traSpeed;
  }

  public static List<TrajPoint> parseTraPointList(JSONArray coordinates, JSONObject properties) {
    ArrayList<TrajPoint> traPointsList = new ArrayList<>();
    JSONArray timestamp = properties.getJSONArray("timestamp");
    JSONArray speedArray = properties.getJSONArray("speed");
    for (int i = 0; i < coordinates.size(); i++) {
      Long sTime = timestamp.getLong(i);
      Double speed = speedArray.getDouble(i);
      HashMap<String, Object> stringObjectMap = new HashMap<>();
      stringObjectMap.put("Time", sTime);
      stringObjectMap.put("Speed", speed);
      TrajPoint trajPoint = new TrajPoint(
          Integer.toString(i),
          DateTimeParse.timeToZonedTime(sTime),
          coordinates.getJSONArray(i).getDouble(0),
          coordinates.getJSONArray(i).getDouble(1),
          stringObjectMap);
      traPointsList.add(trajPoint);
    }
    return traPointsList;
  }


}
