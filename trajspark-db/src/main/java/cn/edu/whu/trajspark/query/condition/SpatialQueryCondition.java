package cn.edu.whu.trajspark.query.condition;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.WKTWriter;

/**
 * 空间查询条件
 *
 * @author Haocheng Wang
 * Created on 2022/9/27
 */
public class SpatialQueryCondition {
  /**
   * Spatial query window geometry, may be geometry collection
   */
  private Envelope queryWindow;

  private SpatialQueryType queryType;

  public SpatialQueryCondition(Envelope queryWindow, SpatialQueryType queryType) {
    this.queryWindow = queryWindow;
    this.queryType = queryType;
  }

  public Envelope getQueryWindow() {
    return queryWindow;
  }

  public String getQueryWindowWKT() {
    WKTWriter writer = new WKTWriter();
    MinimumBoundingBox mbr = new MinimumBoundingBox(queryWindow.getMinX(),
        queryWindow.getMinY(),
        queryWindow.getMaxX(),
        queryWindow.getMaxY());
    return writer.write(mbr.toPolygon(4326));
  }

  public void setQueryWindow(Envelope queryWindow) {
    this.queryWindow = queryWindow;
  }

  public SpatialQueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(SpatialQueryType queryType) {
    this.queryType = queryType;
  }


  /**
   * @author Haocheng Wang
   * Created on 2022/9/27
   *
   * 将查询窗口用于什么样的查询: 两类: 严格包含查询\相交包含查询
   */
  public enum SpatialQueryType {
    /**
     * Query all data that may INTERSECT with query window.
     */
    CONTAIN,
    /**
     * Query all data that is totally contained in query window.
     */
    INTERSECT;
  }
}
