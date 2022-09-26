package cn.edu.whu.trajspark.query.condition;

import cn.edu.whu.trajspark.database.table.TrajectoryTable;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.sfcurve.IndexRange;

import java.util.List;

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

  private TrajectoryTable targetTable;

  public SpatialQueryCondition(Envelope queryWindow, SpatialQueryType queryType, TrajectoryTable geoTable) {
    this.queryWindow = queryWindow;
    this.queryType = queryType;
    this.targetTable = geoTable;
  }

  public Envelope getQueryWindow() {
    return queryWindow;
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

  // TODO: impl
  public List<IndexRange> getIndexIndexRanges() {
    return null;
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
    INCLUDE,
    /**
     * Query all data that is totally contained in query window.
     */
    INTERSECT;
  }
}
