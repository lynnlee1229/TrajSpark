package cn.edu.whu.trajspark.core.operator.store.convertor.basic;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.point.StayPoint;
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
import java.util.Map;
import java.util.Map.Entry;
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
        JSONObject propertiesObject = convertFeatures(trajectory);
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

  public static JSONObject convertStayPointGeoJson(List<StayPoint> stayPoints) {
    JSONObject featureCollection = new JSONObject();
    try {
      featureCollection.put("type", "FeatureCollection");
      JSONArray featureList = new JSONArray();
      for (StayPoint stayPoint : stayPoints) {
        JSONObject feature = new JSONObject();
        JSONObject geometryObject = convertLineString(stayPoint.getPlist());
        JSONObject propertiesObject = convertStayPointFeatures(stayPoint);
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

  public static JSONObject convertStayPointFeatures(StayPoint stayPoint) {
    JSONObject featuresObject = new JSONObject();
    featuresObject.put("sid", stayPoint.getSid());
    featuresObject.put("oid", stayPoint.getOid());
    JSONArray timestampArray  = convertTrajPointTimestamp(stayPoint.getPlist());
    featuresObject.put("timestamp", timestampArray);
    featuresObject.put("startTime", stayPoint.getStartTime().toString());
    featuresObject.put("endTime", stayPoint.getEndTime().toString());
    featuresObject.put("mbr", convertMbr(stayPoint.getMbr()));
    JSONArray coordinateStartArray = convertBaseCoordinate(stayPoint.getCenterPoint());
    featuresObject.put("centerPoint", coordinateStartArray);
    return featuresObject;
  }
  public static JSONObject convertFeatures(Trajectory trajectory) {
    JSONObject featuresObject = new JSONObject();
    TrajFeatures trajectoryFeatures = trajectory.getTrajectoryFeatures();
    featuresObject.put("oid", trajectory.getObjectID());
    featuresObject.put("tid", trajectory.getTrajectoryID());
    JSONArray timestampArray  = convertTrajPointTimestamp(trajectory.getPointList());
    featuresObject.put("timestamp", timestampArray);
    if(!trajectory.isUpdateFeatures()){
      JSONObject trajFeaturesObject = new JSONObject();
      trajFeaturesObject.put("startTime", trajectoryFeatures.getStartTime().toString());
      trajFeaturesObject.put("endTime", trajectoryFeatures.getEndTime().toString());
      JSONArray coordinateStartArray = convertCoordinate(trajectoryFeatures.getStartPoint());
      trajFeaturesObject.put("startPoint", coordinateStartArray);
      JSONArray coordinateEndArray = convertCoordinate(trajectoryFeatures.getEndPoint());
      trajFeaturesObject.put("endPoint", coordinateEndArray);
      trajFeaturesObject.put("pointNum", trajectoryFeatures.getPointNum());
      trajFeaturesObject.put("mbr", convertMbr(trajectoryFeatures.getMbr()));
      trajFeaturesObject.put("speed", trajectoryFeatures.getSpeed());
      trajFeaturesObject.put("len", trajectoryFeatures.getLen());
      featuresObject.put("trajectoryFeatures", trajFeaturesObject);
    }
    if (trajectory.getExtendedValues() != null){
      Map<String, Object> extendedValues = trajectory.getExtendedValues();
      JSONObject extendObject = new JSONObject();
      extendObject.putAll(extendedValues);
      featuresObject.put("extendedValues", extendObject);
    }
    return featuresObject;
  }
  public static JSONArray convertCoordinate(TrajPoint trajPoint){
    JSONArray coordinateArray = new JSONArray();
    coordinateArray.add(trajPoint.getLng());
    coordinateArray.add(trajPoint.getLat());
    return coordinateArray;
  }

  public static JSONArray convertBaseCoordinate(BasePoint trajPoint){
    JSONArray coordinateArray = new JSONArray();
    coordinateArray.add(trajPoint.getLng());
    coordinateArray.add(trajPoint.getLat());
    return coordinateArray;
  }
  public static JSONArray convertTrajPointTimestamp(List<TrajPoint> trajPoints){
    JSONArray coordinateArray = new JSONArray();
    for (TrajPoint trajPoint : trajPoints) {
      coordinateArray.add(trajPoint.getTimestamp().toEpochSecond());
    }
    return coordinateArray;
  }
  public static JSONArray convertMbr (MinimumBoundingBox box){
    JSONArray coordinateArray = new JSONArray();
    List<Double> minCoord = new ArrayList<>();
    List<Double> maxCoord = new ArrayList<>();
    minCoord.add(box.getMinLng());
    minCoord.add(box.getMinLat());
    maxCoord.add(box.getMaxLng());
    maxCoord.add(box.getMaxLat());
    coordinateArray.add(minCoord);
    coordinateArray.add(maxCoord);
    return coordinateArray;
  }

}
