package cn.edu.whu.trajspark.core.operator.load.parser.geojson;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.util.CheckUtils;
import cn.edu.whu.trajspark.core.util.DateUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/10/21
 **/
public class GeoJson2Traj {
  private static final ObjectMapper MAPPER = new ObjectMapper();


  public static Trajectory parse(String geoJson) throws Exception {
    GeoJsonTrajectoryExtend geoJsonTrajectoryExtend =
        MAPPER.readValue(geoJson, GeoJsonTrajectoryExtend.class);
    GeoJsonTrajectoryExtend.Meta meta = geoJsonTrajectoryExtend.getMeta();
    GeoJsonTrajectoryExtend.TrajPoints trajPoints = geoJsonTrajectoryExtend.getTrajPoints();
    String type = trajPoints.getType();
    if (!"FeatureCollection".equals(type)) {
      throw new RuntimeException(
          "feature collection type error! FeatureCollection is expected, but " + type + " is got.");
    } else {
      String trajectoryID = meta.getTrajectoryID();
      String objectID = meta.getObjectID();
      Map<String, Object> extendedValues = meta.getExtendedValues();
      String format = (String) extendedValues.get("format");
      String zoneId = (String) extendedValues.get("zoneId");
      if (format == null) {
        extendedValues.put("format", "yyyy-MM-dd HH:mm:ss");
        format = "yyyy-MM-dd HH:mm:ss";
      }

      if (zoneId == null) {
        extendedValues.put("zoneId", "UTC+8");
        zoneId = "UTC+8";
      }

      DateTimeFormatter formatterUTC =
          DateTimeFormatter.ofPattern(format).withZone(ZoneId.of(zoneId));
      List<TrajPoint> trajPointList = null;
      List<GeoJsonTrajectoryExtend.GeoJsonFeature> features = trajPoints.getFeatures();
      if (!CheckUtils.isCollectionEmpty(features)) {
        trajPointList = new ArrayList(features.size());
        Iterator iter = features.iterator();

        while (iter.hasNext()) {
          GeoJsonTrajectoryExtend.GeoJsonFeature feature =
              (GeoJsonTrajectoryExtend.GeoJsonFeature) iter.next();
          String featureType = feature.getType();
          if ("Feature".equals(featureType)) {
            GeoJsonTrajectoryExtend.Properties properties = feature.getProperties();
            String pid = properties.getPid();
            ZonedDateTime time = DateUtils.parse(properties.getTime(), formatterUTC);
            Map<String, Object> values = properties.getExtendedValues();
            GeoJsonTrajectoryExtend.GeoJsonGeometry geometry = feature.getGeometry();
            String geometryType = geometry.getType();
            Double lng = 0.0;
            Double lat = 0.0;
            if ("Point".equals(geometryType)) {
              List<Double> coordinates = geometry.getCoordinates();
              lng = (Double) coordinates.get(0);
              lat = (Double) coordinates.get(1);
            }

            TrajPoint trajPoint = new TrajPoint(pid, time, lng, lat, values);
            trajPointList.add(trajPoint);
          }
        }
      }

      return new Trajectory(trajectoryID, objectID, trajPointList, extendedValues);
    }
  }
}

