package cn.edu.whu.trajspark.core.operator.analysis.cluster.dbscan;

import cn.edu.whu.trajspark.base.util.GeoUtils;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * @author Lynn Lee
 * @date 2023/1/6
 **/
public class DBScanCluster implements Serializable {
  List<DBScanPoint> srcPoints;
  private Stack<DBScanPoint> subCoreStack;
  boolean isGeodetic;
  private int minPoints;
  protected double radius;
  private List<DBScanPoint> cores;
  private int clusterID = 1;

  /**
   * 构造函数
   *
   * @param srcPoints  待聚类点集
   * @param minPoints  簇最小点数
   * @param radius     簇半径
   * @param isGeodetic 点坐标为是否为投影坐标（投影坐标单位为m）
   */
  public DBScanCluster(List<DBScanPoint> srcPoints, int minPoints, double radius,
                       boolean isGeodetic) {
    this.srcPoints = srcPoints;
    this.minPoints = minPoints;
    this.radius = radius;
    this.isGeodetic = isGeodetic;
    this.cores = new LinkedList();
    this.subCoreStack = new Stack();
  }

  public void doCluster() {
    if (this.srcPoints == null) {
      throw new RuntimeException("srcPoints为空");
    } else {
      this.findCores();

      for (DBScanPoint corePt : this.cores) {
        if (!corePt.isVisited()) {
          corePt.setClusterID(this.clusterID++);
          this.densityConnect(corePt);
        }
      }

    }
  }

  private void findCores() {
    if (this.srcPoints != null) {

      for (DBScanPoint curPt : this.srcPoints) {
        List<DBScanPoint> adjacentPoints = this.getAdjacentPoints(curPt);
        if (adjacentPoints != null && adjacentPoints.size() >= this.minPoints) {
          curPt.setCore(true);
          curPt.setAdjacentPoints(adjacentPoints);
          this.cores.add(curPt);
        }
      }

    }
  }

  private void densityConnect(DBScanPoint corePt) {
    this.subCoreStack.push(corePt);

    while (true) {
      List adjacentPoints;
      do {
        do {
          if (this.subCoreStack.isEmpty()) {
            return;
          }

          DBScanPoint subCore = (DBScanPoint) this.subCoreStack.pop();
          subCore.setVisited(true);
          adjacentPoints = subCore.getAdjacentPoints();
        } while (adjacentPoints == null);
      } while (adjacentPoints.size() == 0);

      for (Object adjacentPoint : adjacentPoints) {
        DBScanPoint adjPt = (DBScanPoint) adjacentPoint;
        if (!adjPt.isVisited()) {
          adjPt.setVisited(true);
          adjPt.setClusterID(corePt.getClusterID());
          if (adjPt.isCore()) {
            this.subCoreStack.push(adjPt);
          }
        }
      }
    }
  }

  protected List<DBScanPoint> getAdjacentPoints(DBScanPoint centerPt) {
    List<DBScanPoint> adjacentPoints = new LinkedList<>();

    for (DBScanPoint refPt : this.srcPoints) {
      double dis;
      if (this.isGeodetic) {
        dis = GeoUtils.getEuclideanDistanceM(centerPt, refPt);
      } else {
        dis = centerPt.distance(refPt);
      }

      if (dis < this.radius) {
        adjacentPoints.add(refPt);
      }
    }

    return adjacentPoints;
  }
}
