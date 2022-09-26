package cn.edu.whu.trajspark.core.common.mbr;

import cn.edu.whu.trajspark.core.common.point.BasePoint;
import cn.edu.whu.trajspark.core.util.CheckUtils;
import cn.edu.whu.trajspark.core.util.GeoUtils;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author Lynn Lee
 * @date 2022/9/8
 **/
public class MinimumBoundingBox extends Envelope {
  private double latGeoLength;
  private double lngGeoLength;
  private boolean withGeoLength;
  private BasePoint centralPoint;

  public MinimumBoundingBox(BasePoint p1, BasePoint p2) {
    super(p1.getLng(), p2.getLng(), p1.getLat(), p2.getLat());
  }

  public MinimumBoundingBox(double lng1, double lat1, double lng2, double lat2) {
    super(lng1, lng2, lat1, lat2);
  }

  public BasePoint getLowerLeft() {
    return new BasePoint(this.getMinX(), this.getMinY());
  }

  public BasePoint getUpperRight() {
    return new BasePoint(this.getMaxX(), this.getMaxY());
  }

  public double getMinLat() {
    return this.getMinY();
  }

  public double getMinLng() {
    return this.getMinX();
  }

  public double getMaxLat() {
    return this.getMaxY();
  }

  public double getMaxLng() {
    return this.getMaxX();
  }

  public BasePoint getCenterPoint() {
    if (this.centralPoint == null) {
      Coordinate var1 = this.centre();
      this.centralPoint = new BasePoint(var1.getX(), var1.getY());
    }

    return this.centralPoint;
  }

  public double getLatGeoLength() {
    if (!this.withGeoLength) {
      this.calcGeoLength();
      this.withGeoLength = true;
    }

    return this.latGeoLength;
  }

  public double getLngGeoLength() {
    if (!this.withGeoLength) {
      this.calcGeoLength();
      this.withGeoLength = true;
    }

    return this.lngGeoLength;
  }

  private void calcGeoLength() {
    BasePoint var1 = new BasePoint(this.getMinLng(), this.getMaxLat());
    this.latGeoLength = GeoUtils.getEuclideanDistance(this.getLowerLeft(), var1);
    var1 = new BasePoint(this.getMaxLng(), this.getMinLat());
    this.lngGeoLength = GeoUtils.getEuclideanDistance(this.getLowerLeft(), var1);
  }

  public boolean isContains(MinimumBoundingBox other) {
    return other == null ? false : this.contains(other);
  }

  public boolean isContains(BasePoint p) {
    return p == null ? false : this.contains(p.getLng(), p.getLat());
  }

  public boolean isIntersects(MinimumBoundingBox other) {
    return other == null ? false : super.intersects(other);
  }

  public MinimumBoundingBox intersects(MinimumBoundingBox other) {
    if (!this.isIntersects(other)) {
      return null;
    } else {
      Envelope intersectionMbr = this.intersection(other);
      return new MinimumBoundingBox(intersectionMbr.getMinX(), intersectionMbr.getMinY(),
          intersectionMbr.getMaxX(), intersectionMbr.getMaxY());
    }
  }

  public double area() {
    return this.getArea();
  }

  public MinimumBoundingBox union(MinimumBoundingBox other) {
    if (other == null) {
      return this;
    } else {
      double minLat = Math.min(this.getMinLat(), other.getMinLat());
      double maxLat = Math.max(this.getMaxLat(), other.getMaxLat());
      double minLng = Math.min(this.getMinLng(), other.getMinLng());
      double maxLng = Math.max(this.getMaxLng(), other.getMaxLng());
      return new MinimumBoundingBox(new BasePoint(minLng, minLat), new BasePoint(maxLng, maxLat));
    }
  }

  public String toString() {
    return "(" + this.getMinLng() + " " + this.getMinLat() + "," + this.getMaxLng() + " "
        + this.getMaxLat() + ")";
  }

  public Polygon toPolygon(int srid) {
    GeometryFactory factory = new GeometryFactory(new PrecisionModel(), srid);
    CoordinateArraySequence
        plist = new CoordinateArraySequence(
        new Coordinate[] {new Coordinate(this.getMinLng(), this.getMaxLat()),
            new Coordinate(this.getMaxLng(), this.getMaxLat()),
            new Coordinate(this.getMaxLng(), this.getMinLat()),
            new Coordinate(this.getMinLng(), this.getMinLat()),
            new Coordinate(this.getMinLng(), this.getMaxLat())});
    LinearRing lineRing = new LinearRing(plist, factory);
    return factory.createPolygon(lineRing);
  }

  public static double getMinDis(MinimumBoundingBox mbr, BasePoint p) {
    return getMBR2PointDis(mbr, p);
  }


  private static double getMBR2PointDis(MinimumBoundingBox mbr, BasePoint p) {
    CheckUtils.checkEmpty(new Object[] {mbr, p});
    double tmpLat = p.getLat();
    double tmpLng = p.getLng();
    if (mbr.isContains(p)) {
      return 0.0;
    } else {
      if (p.getLat() < mbr.getMinLat()) {
        tmpLat = mbr.getMinLat();
      } else if (p.getLat() > mbr.getMaxLat()) {
        tmpLat = mbr.getMaxLat();
      }

      if (p.getLng() < mbr.getMinLng()) {
        tmpLng = mbr.getMinLng();
      } else if (p.getLng() > mbr.getMaxLng()) {
        tmpLng = mbr.getMaxLng();
      }

      BasePoint var7 = new BasePoint(tmpLng, tmpLat);
      return GeoUtils.getEuclideanDistance(var7, p);
    }
  }

  public static MinimumBoundingBox calMinimumBoundingBox(Collection plist) {
    return calMinimumBoundingBox(plist, Function.identity());
  }

  public static MinimumBoundingBox calMinimumBoundingBox(Collection plist, Function func) {
    if (CheckUtils.isCollectionEmpty(plist)) {
      return null;
    } else {
      double minLat = Double.MAX_VALUE;
      double minLng = Double.MAX_VALUE;
      double maxLat = Double.MIN_VALUE;
      double maxLng = Double.MIN_VALUE;
      boolean isMbrNull = true;
      Iterator iter = plist.iterator();

      while (iter.hasNext()) {
        Object next = iter.next();
        BasePoint curP = (BasePoint) func.apply(next);
        if (curP != null) {
          double cLat = curP.getLat();
          double cLng = curP.getLng();
          minLat = Double.min(cLat, minLat);
          minLng = Double.min(cLng, minLng);
          maxLat = Double.max(cLat, maxLat);
          maxLng = Double.max(cLng, maxLng);
          isMbrNull = false;
        }
      }

      if (isMbrNull) {
        return null;
      } else {
        return new MinimumBoundingBox(new BasePoint(minLng, minLat), new BasePoint(maxLng, maxLat));
      }
    }
  }
}