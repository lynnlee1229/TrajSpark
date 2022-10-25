package cn.edu.whu.trajspark.core.operator.load.parser.geojson;

import java.util.List;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/10/21
 **/
public class GeoJsonTrajectoryExtend {
  private TrajPoints trajPoints;
  private Meta meta;

  public GeoJsonTrajectoryExtend() {
  }

  public TrajPoints getTrajPoints() {
    return this.trajPoints;
  }

  public void setTrajPoints(TrajPoints trajPoints) {
    this.trajPoints = trajPoints;
  }

  public Meta getMeta() {
    return this.meta;
  }

  public void setMeta(Meta meta) {
    this.meta = meta;
  }

  public static class Meta {
    private String trajectoryID;
    private String objectID;
    private Map<String, Object> extendedValues;

    public Meta() {
    }

    public String getTrajectoryID() {
      return this.trajectoryID;
    }

    public void setTrajectoryID(String trajectoryID) {
      this.trajectoryID = trajectoryID;
    }

    public String getObjectID() {
      return this.objectID;
    }

    public void setObjectID(String objectID) {
      this.objectID = objectID;
    }

    public Map<String, Object> getExtendedValues() {
      return this.extendedValues;
    }

    public void setExtendedValues(Map<String, Object> extendedValues) {
      this.extendedValues = extendedValues;
    }
  }
  public static class Properties {
    private String pid;
    private String time;
    private Map<String, Object> extendedValues;

    public Properties() {
    }

    public String getPid() {
      return this.pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    public String getTime() {
      return this.time;
    }

    public void setTime(String time) {
      this.time = time;
    }

    public Map<String, Object> getExtendedValues() {
      return this.extendedValues;
    }

    public void setExtendedValues(Map<String, Object> extendedValues) {
      this.extendedValues = extendedValues;
    }
  }
  public static class GeoJsonGeometry {
    private String type = "Point";
    private List<Double> coordinates;

    public GeoJsonGeometry() {
    }

    public String getType() {
      return this.type;
    }

    public List<Double> getCoordinates() {
      return this.coordinates;
    }

    public void setCoordinates(List<Double> coordinates) {
      this.coordinates = coordinates;
    }
  }
  public static class GeoJsonFeature {
    private String type = "Feature";
    private GeoJsonGeometry geometry;
    private Properties properties;

    public GeoJsonFeature() {
    }

    public String getType() {
      return this.type;
    }

    public GeoJsonGeometry getGeometry() {
      return this.geometry;
    }

    public void setGeometry(GeoJsonGeometry geometry) {
      this.geometry = geometry;
    }

    public Properties getProperties() {
      return this.properties;
    }

    public void setProperties(Properties properties) {
      this.properties = properties;
    }
  }
  public static class TrajPoints {
    private String type = "FeatureCollection";
    private List<GeoJsonFeature> features;

    public TrajPoints() {
    }

    public String getType() {
      return this.type;
    }

    public List<GeoJsonFeature> getFeatures() {
      return this.features;
    }

    public void setFeatures(List<GeoJsonFeature> features) {
      this.features = features;
    }
  }

}
