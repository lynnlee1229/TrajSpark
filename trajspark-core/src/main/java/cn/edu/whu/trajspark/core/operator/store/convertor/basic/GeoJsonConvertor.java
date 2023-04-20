package cn.edu.whu.trajspark.core.operator.store.convertor.basic;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoJsonConvertor {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJsonConvertor.class);

  public static JSONObject convertGeoJson(List<Trajectory> trajectoryList) {
    JSONObject featureCollection = new JSONObject();
    try {
      featureCollection.put("type", "FeatureCollection");
      JSONArray featureList = new JSONArray();
      for (Trajectory trajectory : trajectoryList) {
        JSONObject feature = new JSONObject();
        JSONObject geometryObject = convertLineString(trajectory.getPointList());
        JSONObject propertiesObject = convertFeatures(trajectory.getTrajectoryFeatures());
        feature.put("type", "Feature");
        feature.put("geometry", geometryObject);
        feature.put("properties", propertiesObject);
        featureList.add(feature);
      }
      featureCollection.put("features", featureList);
    } catch (JSONException e) {
      LOGGER.info("can't save json object: " + e.toString());
    }
    return featureCollection;
  }

  public static JSONObject convertLineString(List<TrajPoint> pointList) {
    JSONObject geometryObject = new JSONObject();
    JSONArray coordinateArray = new JSONArray();
    for (TrajPoint trajPoint : pointList) {
      List<Double> coordtemp = new ArrayList<>();
      coordtemp.add(trajPoint.getLng());
      coordtemp.add(trajPoint.getLat());
      coordinateArray.add(coordtemp);
    }
    geometryObject.put("coordinates", coordinateArray);
    geometryObject.put("type", "LineString");
    return geometryObject;
  }

  public static JSONObject convertFeatures(TrajFeatures trajectoryFeatures) {
    JSONObject featuresObject = new JSONObject();
    featuresObject.put("startTime", trajectoryFeatures.getStartTime().toString());
    featuresObject.put("endTime", trajectoryFeatures.getEndTime().toString());
    JSONArray coordinateStartArray = convertCoordinate(trajectoryFeatures.getStartPoint());
    featuresObject.put("startPoint", coordinateStartArray);
    JSONArray coordinateEndArray = convertCoordinate(trajectoryFeatures.getEndPoint());
    featuresObject.put("endPoint", coordinateEndArray);
    featuresObject.put("pointNum", trajectoryFeatures.getPointNum());
    featuresObject.put("mbr", trajectoryFeatures.getMbr().toString());
    featuresObject.put("speed", trajectoryFeatures.getSpeed());
    featuresObject.put("len", trajectoryFeatures.getLen());
    return featuresObject;
  }
  public static JSONArray convertCoordinate(TrajPoint trajPoint){
    JSONArray coordinateArray = new JSONArray();
    coordinateArray.add(trajPoint.getLng());
    coordinateArray.add(trajPoint.getLat());
    return coordinateArray;
  }

}
