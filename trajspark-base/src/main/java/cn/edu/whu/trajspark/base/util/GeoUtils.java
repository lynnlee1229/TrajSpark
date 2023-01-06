package cn.edu.whu.trajspark.base.util;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.point.TrajPoint;
import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.ShapeFactory;

/**
 * @author Lynn Lee
 * @date 2022/9/8
 **/
public class GeoUtils implements Serializable {
  public static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;
  public static final DistanceCalculator DISTANCE_CALCULATOR = SPATIAL_CONTEXT.getDistCalc();
  public static final ShapeFactory SHAPE_FACTORY = SPATIAL_CONTEXT.getShapeFactory();

  private static final double MIN_LNG = -180.;
  private static final double MAX_LNG = 180.;
  private static final double MIN_LAT = -90.;
  private static final double MAX_LAT = 90.;

  public static double distanceToDEG(double distance) {
    return distance * DistanceUtils.KM_TO_DEG;
  }

  /**
   * Calculate distance of two geometries. If the geometry is not point, use the centroid
   * of the geometry to calculate.
   */
  public static double getEuclideanDistanceKM(Geometry geom1, Geometry geom2) {
    org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
    org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
    return getEuclideanDistanceKM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
  }

  public static double getEuclideanDistanceM(Geometry geom1, Geometry geom2) {
    org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
    org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
    return getEuclideanDistanceM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
  }

  public static double getEuclideanDistanceKM(double lng1, double lat1, double lng2, double lat2) {
    double lat1Rad = Math.toRadians(lat1);
    double lat2Rad = Math.toRadians(lat2);
    double deltaLat = lat1Rad - lat2Rad;
    double deltaLng = Math.toRadians(lng1) - Math.toRadians(lng2);
    return 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(deltaLat / 2.0), 2.0)
        + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.pow(Math.sin(deltaLng / 2.0), 2.0)))
        * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM;
  }

  public static double getEuclideanDistanceM(double lng1, double lat1, double lng2, double lat2) {
    double lat1Rad = Math.toRadians(lat1);
    double lat2Rad = Math.toRadians(lat2);
    double deltaLat = lat1Rad - lat2Rad;
    double deltaLng = Math.toRadians(lng1) - Math.toRadians(lng2);
    return 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(deltaLat / 2.0), 2.0)
        + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.pow(Math.sin(deltaLng / 2.0), 2.0)))
        * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000;
  }

  public static double getGeoListLen(List<Geometry> geoList) {
    double len = 0.0;
    for (int i = 1; i < geoList.size(); i++) {
      len += getEuclideanDistanceKM(geoList.get(i - 1).getCentroid(), geoList.get(i).getCentroid());
    }
    return len;
  }

  public static double getTrajListLen(List<TrajPoint> trajList) {
    double len = 0.0;
    for (int i = 1; i < trajList.size(); i++) {
      len +=
          getEuclideanDistanceKM(trajList.get(i - 1).getCentroid(), trajList.get(i).getCentroid());
    }
    return len;
  }

  /**
   * Calculate the envelop.
   *
   * @param p   the center point
   * @param dis distance km
   */
  public static Envelope getEnvelopeByDis(org.locationtech.jts.geom.Point p, double dis) {
    return getEnvelopeByDis(p.getX(), p.getY(), dis);
  }

  public static Envelope getEnvelopeByDis(double lng, double lat, double dis) {
    Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
    Rectangle rect = DISTANCE_CALCULATOR.calcBoxByDistFromPt(point, dis * DistanceUtils.KM_TO_DEG,
        SPATIAL_CONTEXT, null);
    return new Envelope(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
  }

  public static Coordinate getPointOnBearing(double lng, double lat, double angle, double dis) {
    Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
    Point result = DISTANCE_CALCULATOR.pointOnBearing(point, dis * DistanceUtils.KM_TO_DEG, angle,
        SPATIAL_CONTEXT, null);
    return new Coordinate(result.getX(), result.getY());
  }

  private static double checkLng(double lng) {
    if (lng < MIN_LNG) {
      return MIN_LNG;
    }
    return Math.min(lng, MAX_LNG);
  }

  private static double checkLat(double lat) {
    if (lat < MIN_LAT) {
      return MIN_LAT;
    }
    return Math.min(lat, MAX_LAT);
  }

  public static double getKmFromDegree(double km) {
    return km * DistanceUtils.KM_TO_DEG;
  }

  public static double getDegreeFromKm(double degree) {
    return degree * DistanceUtils.DEG_TO_KM;
  }

  public static double getSpeed(TrajPoint p1, TrajPoint p2) {
    long timeSpanInSec = Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p2.getTimestamp()));
    if (timeSpanInSec == 0L) {
      return 0.0;
    } else {
      double distanceInM = getEuclideanDistanceM(p1, p2);
      return distanceInM / (double) timeSpanInSec * 3.6;
    }
  }

  public static double getSpeed(TrajPoint p1, TrajPoint p2, TrajPoint p3) {
    long timeSpanInSec = Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p3.getTimestamp()));
    if (timeSpanInSec == 0L) {
      return 0.0;
    } else {
      double distanceInM = getEuclideanDistanceM(p1, p2) + getEuclideanDistanceM(p2, p3);
      return distanceInM / (double) timeSpanInSec * 3.6;
    }
  }

  public static double getDeltaV(TrajPoint p1, TrajPoint p2, TrajPoint p3) {
    long timeSpanInSec2 = Math.abs(ChronoUnit.SECONDS.between(p2.getTimestamp(), p3.getTimestamp()));
    if (timeSpanInSec2 == 0) {
      return 0.0;
    }
    long timeSpanInSec1 = Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p3.getTimestamp()));
    double v1 = timeSpanInSec1 == 0 ? 0.0 : getEuclideanDistanceM(p1, p2) / (double) timeSpanInSec1;
    double v2 = getEuclideanDistanceM(p2, p3) / (double) timeSpanInSec2;
    double deltaV = (v2 - v1) / timeSpanInSec2;
    return (v2 - v1) / timeSpanInSec2;
  }

  public static MinimumBoundingBox calMinimumBoundingBox(List geoList) {
    if (geoList != null && !geoList.isEmpty()) {
      double latMin = Double.MAX_VALUE;
      double lngMin = Double.MAX_VALUE;
      double latMax = Double.MIN_VALUE;
      double lngMax = Double.MIN_VALUE;
      double tmpLng, tmpLat;
      for (Iterator iter = geoList.iterator(); iter.hasNext();
           lngMax = Double.max(tmpLng, lngMax)) {
        BasePoint tmpP = (BasePoint) iter.next();
        tmpLat = tmpP.getLat();
        tmpLng = tmpP.getLng();
        latMin = Double.min(tmpLat, latMin);
        lngMin = Double.min(tmpLng, lngMin);
        latMax = Double.max(tmpLat, latMax);
      }

      return new MinimumBoundingBox(new BasePoint(lngMin, latMin), new BasePoint(lngMax, latMax));
    } else {
      return null;
    }
  }

  public static double getEuclideanDistance(TrajPoint p0, TrajPoint p1) {
    double dx = p1.getX() - p0.getX();
    double dy = p1.getY() - p0.getY();
    return Math.sqrt((dx * dx + dy * dy));
  }

  public static double getAngle(TrajPoint p0, TrajPoint p1, TrajPoint p2) {
    double d1 = getEuclideanDistance(p0, p1);
    double d2 = getEuclideanDistance(p1, p2);
    // 三点中至少有1静止点，返回180度
    if (d1 * d2 == 0) {
      return 180.0;
    }
    double x1 = p1.getX() - p0.getX();
    double y1 = p1.getY() - p0.getY();
    double x2 = p2.getX() - p1.getX();
    double y2 = p2.getY() - p1.getY();
    double delta = (x1 * x2 + y1 * y2) / (d1 * d2);
    // 边界值问题处理
    if (Math.abs(delta - 1.0) < 1e-10) {
      delta = 1.0;
    }
    if (Math.abs(delta + 1.0) < 1e-10) {
      delta = -1.0;
    }
    return Math.toDegrees(Math.acos(delta));
  }

  public static double getRatio(TrajPoint p0, TrajPoint p1, TrajPoint p2) {
    double d1 = getEuclideanDistanceM(p0, p1);
    double d2 = getEuclideanDistanceM(p1, p2);
    double d3 = getEuclideanDistanceM(p0, p2);
    return (d1 + d2) / d3;
  }

  public static TrajPoint fixPos(TrajPoint preP, TrajPoint curP, TrajPoint nextP) {
    double tSum = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), nextP.getTimestamp()));
    double t = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), curP.getTimestamp()));
    if (t != 0) {
      curP.setLng(preP.getLng() + (nextP.getLng() - preP.getLng()) * t / tSum);
      curP.setLat(preP.getLat() + (nextP.getLat() - preP.getLat()) * t / tSum);

    } else {
      curP.setLng(preP.getLng());
      curP.setLat(preP.getLat());
    }
    return curP;
  }
}
