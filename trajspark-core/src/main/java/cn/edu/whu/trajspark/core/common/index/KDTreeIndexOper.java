package cn.edu.whu.trajspark.core.common.index;

import cn.edu.whu.trajspark.base.util.GeoUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.kdtree.KdNode;
import org.locationtech.jts.index.kdtree.KdTree;

/**
 * @author Lynn Lee
 * @date 2023/1/6
 **/
public class KDTreeIndexOper<T extends Point> implements Serializable {
  private static final double EARTH_RADIUS_IN_METER = 6378137.0;
  private KdTree kdTree = new KdTree();

  public KDTreeIndexOper(List<T> points) {

    for (T t : points) {
      this.kdTree.insert(((Point) t).getCoordinate(), (Point) t);
    }

  }

  public List queryFenceInclude(Point centPt, double distance, boolean isGeodetic) {
    Envelope fence;
    if (isGeodetic) {
      fence = this.getGeoFence(centPt, distance);
    } else {
      fence =
          new Envelope(centPt.getX() - distance, centPt.getX() + distance, centPt.getY() - distance,
              centPt.getY() + distance);
    }

    List nodes = this.kdTree.query(fence);
    if (nodes != null && nodes.size() != 0) {
      List<Point> candidatePoints = new ArrayList<>(nodes.size());

      for (Object o : nodes) {
        candidatePoints.add((Point) ((Point) ((KdNode) o).getData()));
      }

      List resultPoints;
      if (isGeodetic) {
        resultPoints = candidatePoints.stream().filter((point) -> {
          return
              GeoUtils.getEuclideanDistanceM(point.getX(), point.getY(), centPt.getX(),
                  centPt.getY()) < distance;
        }).collect(Collectors.toList());
      } else {
        resultPoints = candidatePoints.stream().filter((point) -> {
          return GeoUtils.getEuclideanDistance(point.getX(), point.getY(), centPt.getX(),
              centPt.getY()) < distance;
        }).collect(Collectors.toList());
      }

      return resultPoints;
    } else {
      return null;
    }
  }

  private Envelope getGeoFence(Point centPt, double distance) {
    double perimeter = 4.007501668557849E7;
    double latPerM = 360.0 / perimeter;
    double lngPerM = 360.0 / (perimeter * Math.cos(centPt.getY()));
    double latBuffLen = distance * latPerM;
    double lngBuffLen = distance * lngPerM;
    return new Envelope(centPt.getX() - lngBuffLen, centPt.getX() + lngBuffLen,
        centPt.getY() - latBuffLen, centPt.getY() + latBuffLen);
  }
}
