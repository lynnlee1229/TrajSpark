package cn.edu.whu.trajspark.core.operator.analysis.cluster.dbscan;

import cn.edu.whu.trajspark.base.util.GeoUtils;
import cn.edu.whu.trajspark.core.common.index.RTreeIndexOper;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

/**
 * @author Lynn Lee
 * @date 2023/1/6
 **/
public class DBScanClusterRTree extends DBScanCluster {
  private RTreeIndexOper rTreeIndexOper;

  public DBScanClusterRTree(List<DBScanPoint> srcPoints, int minPoints, double radius,
                            boolean isGeodetic) {
    super(srcPoints, minPoints, radius, isGeodetic);
    this.buildIndex();
  }

  private void buildIndex() {
    if (this.srcPoints != null) {
      this.rTreeIndexOper = new RTreeIndexOper();

      for (DBScanPoint dbScanPoint : this.srcPoints) {
        this.rTreeIndexOper.add(dbScanPoint);
      }

      this.rTreeIndexOper.buildIndex();
    }
  }

  protected List<DBScanPoint> getAdjacentPoints(DBScanPoint centerPt) {
    if (this.rTreeIndexOper == null) {
      super.getAdjacentPoints(centerPt);
    }

    double bufLen;
    if (this.isGeodetic) {
      bufLen = GeoUtils.getDegreeFromKm(this.radius / 1000.0);
    } else {
      bufLen = this.radius;
    }

    List<DBScanPoint> adjacentPoints = null;
    Geometry bufferGeom = centerPt.buffer(bufLen);
    if (bufferGeom instanceof Polygon) {
      Polygon buffer = (Polygon) bufferGeom;
      adjacentPoints =
          (List) this.rTreeIndexOper.searchIntersect(buffer, false).stream().map((g) -> {
            return (DBScanPoint) g;
          }).collect(Collectors.toList());
    }

    return adjacentPoints;
  }
}
