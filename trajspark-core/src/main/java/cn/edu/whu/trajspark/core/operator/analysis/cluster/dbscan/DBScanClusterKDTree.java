package cn.edu.whu.trajspark.core.operator.analysis.cluster.dbscan;

import cn.edu.whu.trajspark.core.common.index.KDTreeIndexOper;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/1/6
 **/
public class DBScanClusterKDTree extends DBScanCluster {
  private KDTreeIndexOper<DBScanPoint> kdTreeIndexOper;

  public DBScanClusterKDTree(List<DBScanPoint> srcPoints, int minPoints, double radius,
                             boolean isGeodetic) {
    super(srcPoints, minPoints, radius, isGeodetic);
    this.buildIndex();
  }

  private void buildIndex() {
    if (this.srcPoints != null) {
      this.kdTreeIndexOper = new KDTreeIndexOper<>(this.srcPoints);
    }
  }

  protected List<DBScanPoint> getAdjacentPoints(DBScanPoint centerPt) {
    return this.kdTreeIndexOper == null ? super.getAdjacentPoints(centerPt)
        : this.kdTreeIndexOper.queryFenceInclude(centerPt, this.radius, this.isGeodetic);
  }
}
