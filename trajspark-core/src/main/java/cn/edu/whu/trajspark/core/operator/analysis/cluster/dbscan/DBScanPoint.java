package cn.edu.whu.trajspark.core.operator.analysis.cluster.dbscan;

import cn.edu.whu.trajspark.base.point.BasePoint;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author Lynn Lee
 * @date 2023/1/6
 **/
public class DBScanPoint extends BasePoint {
  private int id;
  private int clusterID;
  private boolean isVisited;
  private boolean isCore;
  private List<DBScanPoint> adjacentPoints;

  public int getId() {
    return this.id;
  }

  public int getClusterID() {
    return this.clusterID;
  }

  public void setClusterID(int clusterID) {
    this.clusterID = clusterID;
  }

  boolean isVisited() {
    return this.isVisited;
  }

  void setVisited(boolean visited) {
    this.isVisited = visited;
  }

  public boolean isCore() {
    return this.isCore;
  }

  public void setCore(boolean core) {
    this.isCore = core;
  }

  List<DBScanPoint> getAdjacentPoints() {
    return this.adjacentPoints;
  }

  void setAdjacentPoints(List<DBScanPoint> adjacentPoints) {
    this.adjacentPoints = adjacentPoints;
  }

  public DBScanPoint(int id, double x, double y, int srid) {
    super(new CoordinateArraySequence(new Coordinate[]{new Coordinate(x, y)}), new GeometryFactory(new PrecisionModel(), srid));
    this.id = id;
    this.clusterID = 0;
    this.isVisited = false;
    this.isCore = false;
  }

  public String toString() {
    return this.id + " " + this.getX() + " " + this.getY() + " " + this.clusterID + "\n";
  }
}
