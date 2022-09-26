package cn.edu.whu.trajspark.core.common.trajectory;

import cn.edu.whu.trajspark.core.common.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.util.GeoUtils;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author Lynn Lee
 * @date 2022/9/7
 **/
public class Trajectory implements Serializable {
  private String trajectoryID;
  private String objectID;
  private List<TrajPoint> pointList;

  private TrajFeatures trajectoryFeatures;
  private boolean updateFeatures = true;
  private boolean updateLineString = true;

  private boolean updatePointListId = true;
  private Map<String, Object> extendedValues;
  private LineString lineString;

  public Trajectory(String trajectoryID, String objectID, List<TrajPoint> pointList,
                    TrajFeatures trajectoryFeatures) {
    this.trajectoryID = trajectoryID;
    this.objectID = objectID;
    this.pointList = pointList;
    this.trajectoryFeatures = trajectoryFeatures;
    this.updateFeatures = false;
  }

  public Trajectory(String trajectoryID, String objectID, List<TrajPoint> pointList,
                    TrajFeatures trajectoryFeatures, Map<String, Object> extendedValues) {
    this.trajectoryID = trajectoryID;
    this.objectID = objectID;
    this.pointList = pointList;
    this.trajectoryFeatures = trajectoryFeatures;
    this.extendedValues = extendedValues;
    this.updateFeatures = false;
  }

  public Trajectory(String trajectoryID, String objectID, List<TrajPoint> pointList,
                    Map<String, Object> extendedValues) {
    this.trajectoryID = trajectoryID;
    this.objectID = objectID;
    this.pointList = pointList;
    this.extendedValues = extendedValues;
  }

  public Trajectory(String trajectoryID, String objectID, List<TrajPoint> pointList) {
    this.trajectoryID = trajectoryID;
    this.objectID = objectID;
    this.pointList = pointList;
  }

  public void addPoint(TrajPoint point) {
    if (this.pointList == null || this.pointList.isEmpty()) {
      this.pointList = new LinkedList();
    }

    this.pointList.add(point);
    this.updateFeatures = true;
    this.updateLineString = true;
    this.updatePointListId = true;
  }

  public void setTrajectoryID(String trajectoryID) {
    this.trajectoryID = trajectoryID;
  }

  public void addPoints(List<TrajPoint> points) {
    if (this.pointList == null || this.pointList.isEmpty()) {
      this.pointList = new LinkedList();
    }

    this.pointList.addAll(points);
    this.updateFeatures = true;
    this.updateLineString = true;
    this.updatePointListId = true;
  }

  public LineString getLineString() {
    if (this.updateLineString) {
      this.updateLineString();
    }

    return this.lineString;
  }

  public String getTrajectoryID() {
    return this.trajectoryID;
  }

  public String getObjectID() {
    return this.objectID;
  }

  public List<TrajPoint> getPointList() {
    return this.pointList;
  }

  public List<TrajPoint> getUpdatedPointList() {
    if (this.updatePointListId) {
      this.updatePointListId();
    }
    return this.pointList;
  }

  public Map<String, Object> getExtendedValues() {
    return this.extendedValues;
  }

  public void setExtendedValues(Map<String, Object> extendedValues) {
    this.extendedValues = extendedValues;
  }

  public void setSRID(int srid) {
    this.getLineString().setSRID(srid);
  }

  public int getSRID() {
    return this.getLineString().getSRID();
  }

  public TrajFeatures getTrajectoryFeatures() {
    if (this.updateFeatures && this.pointList != null && !this.pointList.isEmpty()) {
      this.updateFeature();
      this.updateFeatures = false;
    }

    return this.trajectoryFeatures;
  }

  private void updateFeature() {
    this.pointList.sort((o1, o2) -> {
      return (int) (o1.getTimestamp().toEpochSecond() - o2.getTimestamp().toEpochSecond());
    });
    ZonedDateTime startTime = ((TrajPoint) this.pointList.get(0)).getTimestamp();
    ZonedDateTime endTime =
        ((TrajPoint) this.pointList.get(this.pointList.size() - 1)).getTimestamp();
    double length = 0.0;
    double minLat = Double.MAX_VALUE;
    double minLng = Double.MAX_VALUE;
    double maxLat = Double.MIN_VALUE;
    double maxLng = Double.MIN_VALUE;

    TrajPoint p;
    for (Iterator iter = this.pointList.iterator(); iter.hasNext();
         maxLng = Math.max(maxLng, p.getLng())) {
      p = (TrajPoint) iter.next();
      minLat = Math.min(minLat, p.getLat());
      minLng = Math.min(minLng, p.getLng());
      maxLat = Math.max(maxLat, p.getLat());
    }

    MinimumBoundingBox mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat);
    double hour = (double) (endTime.toEpochSecond() - startTime.toEpochSecond()) / 60.0 / 60.0;
    double speed = length / hour;
    this.trajectoryFeatures =
        new TrajFeatures(startTime, endTime, (TrajPoint) this.pointList.get(0),
            (TrajPoint) this.pointList.get(this.pointList.size() - 1), this.pointList.size(), mbr,
            speed, length);
  }

  private void updateLineString() {
    if (this.pointList != null) {
      int srid = this.lineString == null ? 4326 : this.lineString.getSRID();
      this.lineString = new LineString(new CoordinateArraySequence(
          (Coordinate[]) ((List) this.pointList.stream().map((gpsPoint) -> {
            return new Coordinate(gpsPoint.getLng(), gpsPoint.getLat());
          }).collect(
              Collectors.toList())).toArray(new Coordinate[0])),
          new GeometryFactory(new PrecisionModel(), srid));
      this.updateLineString = false;
    }
  }

  private void updatePointListId() {
    for (int i = 0; i < pointList.size(); ++i) {
      pointList.get(i).setPid(String.valueOf(i));
    }
  }

  public boolean isIntersect(Trajectory otherTrajectory) {
    LineString otherLine = otherTrajectory.getLineString();
    return otherLine != null && otherLine.getNumPoints() != 0 ?
        this.getLineString().intersects(otherLine) : false;
  }

  public boolean isPassPoint(Point point, double distance) {
    if (this.getLineString() != null && point != null) {
      double degree = GeoUtils.getDegreeFromKm(distance);
      return this.getLineString().intersects(point.buffer(degree));
    } else {
      return false;
    }
  }


  public Polygon buffer(double distance) {
    if (this.getLineString() == null) {
      return null;
    } else {
      double degree = GeoUtils.getDegreeFromKm(distance);
      return (Polygon) this.getLineString().buffer(degree);
    }
  }

  public Polygon convexHull() {
    return this.getLineString() == null ? null : (Polygon) this.getLineString().convexHull();
  }

  @Override
  public String toString() {
    return "{Trajector:[traj_id:" + getTrajectoryID() + " object_id:" + getObjectID() + "]}";
  }

  public static class Schema {
    public static final String TRAJECTORY_ID = "trajectory_id";
    public static final String OBJECT_ID = "object_id";
    public static final String TRAJ_POINT = "traj_point";
    public static final String TRAJ_LIST = "traj_list";
    public static final String MBR = "mbr";
    public static final String TIMESTAMP = "timestamp";
    public static final String LAT = "lat";
    public static final String LNG = "lng";
    public static final String TRAJ_POINT_ID = "traj_point_id";
    public static final String START_TIME = "start_time";
    public static final String END_TIME = "end_time";
    public static final String START_POSITION = "start_position";
    public static final String END_POSITION = "end_position";
    public static final String POINT_NUMBER = "point_number";
    public static final String TIME_RANGE_KEY = "time_range";
    public static final String SPEED = "speed";
    public static final String LENGTH = "length";
    public static final String SIGNATURE = "signature";

    public Schema() {
    }

    public static Set<String> defaultNameSet() throws IllegalAccessException {
      Set<String> defaultNames = new HashSet();
      Class clazz = Schema.class;
      Field[] fields = clazz.getFields();
      Field[] tmpFields = fields;
      int nFields = fields.length;

      for (int i = 0; i < nFields; ++i) {
        Field field = tmpFields[i];
        defaultNames.add(field.get(clazz).toString());
      }

      return defaultNames;
    }
  }
}
