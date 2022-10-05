package cn.edu.whu.trajspark.index;

import cn.edu.whu.trajspark.index.conf.Constants;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.locationtech.geomesa.curve.XZ2SFC;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.sfcurve.IndexRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *  包装XZ2SFC, 负责 <br/>
 *  1. 接收一个轨迹/bb,输出xz2 code.<br/>
 *  2. 接收一个空间范围 + 查询条件, 输出intervals
 *
 * @author Haocheng Wang
 * Created on 2022/9/26
 */
public class XZ2PlusIndex {

  private static final Logger logger = LoggerFactory.getLogger(XZ2PlusIndex.class);

  private static final XZ2SFC xz2Sfc = XZ2SFC.apply(Constants.MAX_XZ2_PRECESION);

  /**
   * Get xz2 index for the line string.
   *
   * @param lineString Line string to be indexed.
   * @return The XZ2 code for the
   */
  long index(LineString lineString) {
    return index(lineString.getEnvelopeInternal());
  }


  long index(Envelope boundingBox) {
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    // lenient is false so the points out of boundary can throw exception.
    return xz2Sfc.index(minLng, maxLng, minLat, maxLat, false);
  }

  /**
   * Get index ranges of the query range, support two spatial query types
   * TODO: impl
   * @param spatialQueryRange Spatial query on the index.
   * @return List of xz2 index ranges corresponding to the query range.
   */
  List<IndexRange> ranges(SpatialQueryCondition spatialQueryRange) {
    List<IndexRange> indexRangeList = new ArrayList<>(20);
    if (spatialQueryRange.getQueryType() == SpatialQueryCondition.SpatialQueryType.INTERSECT) {
      // TODO: XZ2内置的查询算法
    } else if (spatialQueryRange.getQueryType() == SpatialQueryCondition.SpatialQueryType.INCLUDE) {
      // TODO: 严格包含查询
    } else {
      logger.error("Spatial query type: {} is not supported", spatialQueryRange.getQueryType());
      throw new IllegalArgumentException();
    }
    return indexRangeList;
  }
}
