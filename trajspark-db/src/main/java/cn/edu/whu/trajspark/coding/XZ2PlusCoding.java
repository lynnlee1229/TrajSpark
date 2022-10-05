package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.coding.conf.Constants;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.locationtech.geomesa.curve.XZ2SFC;
import org.locationtech.jts.geom.*;
import org.locationtech.sfcurve.IndexRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;

import static cn.edu.whu.trajspark.coding.conf.Constants.*;

/**
 *  包装XZ2SFC, 负责 <br/>
 *  1. 接收一个轨迹/bb,输出xz2 code.<br/>
 *  2. 接收一个空间范围 + 查询条件, 输出intervals
 *
 * @author Haocheng Wang
 * Created on 2022/9/26
 */
public class XZ2PlusCoding implements SpatialCoding {

  private static final Logger logger = LoggerFactory.getLogger(XZ2PlusCoding.class);

  private XZ2SFC xz2Sfc;

  short xz2Precision;

  public XZ2PlusCoding() {
    xz2Precision = Constants.MAX_XZ2_PRECISION;
    xz2Sfc = XZ2SFC.apply(xz2Precision);
  }

  public XZ2PlusCoding(short xz2Precision) {
    if (xz2Precision > Constants.MAX_XZ2_PRECISION) {
      logger.warn("The XZ2 precision you provided exceeds max {}, we automatically set it to max precision.", Constants.MAX_XZ2_PRECISION);
      xz2Precision = Constants.MAX_XZ2_PRECISION;
    } else {
      this.xz2Precision = xz2Precision;
    }
    xz2Sfc = XZ2SFC.apply(xz2Precision);
  }

  public int getXz2Precision() {
    return xz2Precision;
  }

  /**
   * Get xz2 index for the line string.
   *
   * @param lineString Line string to be indexed.
   * @return The XZ2 code for the
   */
  public long index(LineString lineString) {
    return index(lineString.getEnvelopeInternal());
  }


  public long index(Envelope boundingBox) {
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    // lenient is false so the points out of boundary can throw exception.
    return xz2Sfc.index(minLng, minLat, maxLng, maxLat, false);
  }

  /**
   * Get index ranges of the query range, support two spatial query types
   * @param spatialQueryRange Spatial query on the index.
   * @return List of xz2 index ranges corresponding to the query range.
   */
  public List<IndexRange> ranges(SpatialQueryCondition spatialQueryRange) {
    List<IndexRange> indexRangeList = new ArrayList<>(100);
    Envelope envelope = spatialQueryRange.getQueryWindow();
    double xMin = envelope.getMinX();
    double yMin = envelope.getMinY();
    double xMax = envelope.getMaxX();
    double yMax = envelope.getMaxY();
    if (spatialQueryRange.getQueryType() == SpatialQueryCondition.SpatialQueryType.INTERSECT) {
      indexRangeList = JavaConverters.<IndexRange>seqAsJavaList(xz2Sfc.ranges(xMin, yMin, xMax, yMax));
    } else if (spatialQueryRange.getQueryType() == SpatialQueryCondition.SpatialQueryType.INCLUDE) {
      // TODO: 严格包含查询
      logger.error("Spatial query type: {} is not supported", spatialQueryRange.getQueryType());
      throw new IllegalArgumentException();
    }
    return indexRangeList;
  }

  /**
   * TODO: Get the spatial polygon managed by the spatial coding.
   * @param coding Spatial coding value generated by this coding strategy.
   * @return Polygon represented by the spatial coding, have some info in userdata.
   */
  public Polygon getSpatialPolygon(long coding) {
    // 1. get sequence
    List<Integer> list = getSequence(coding);

    double xmax = 1.0;
    double xmin = 0.0;
    double ymax = 1.0;
    double ymin = 0.0;

    // 2. get grid
    for (Integer integer : list) {
      double xCenter = (xmax + xmin) / 2;
      double yCenter = (ymax + ymin) / 2;
      switch (integer) {
        case 0:
          xmax = xCenter;
          ymax = yCenter;
          break;
        case 1:
          xmin = xCenter;
          ymax = yCenter;
          break;
        case 2:
          xmax = xCenter;
          ymin = yCenter;
          break;
        default:
          xmin = xCenter;
          ymin = yCenter;
      }
    }

    // 3. denormalize
    xmax = (XZ2_X_MAX - XZ2_X_MIN) * xmax + XZ2_X_MIN;
    xmin = (XZ2_X_MAX - XZ2_X_MIN) * xmin + XZ2_X_MIN;
    ymax = (XZ2_Y_MAX - XZ2_Y_MIN) * ymax + XZ2_Y_MIN;
    ymin = (XZ2_Y_MAX - XZ2_Y_MIN) * ymin + XZ2_Y_MIN;
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(xmin, ymin);
    coordinates[1] = new Coordinate(xmin, ymax);
    coordinates[2] = new Coordinate(xmax, ymax);
    coordinates[3] = new Coordinate(xmax, ymin);
    coordinates[4] = new Coordinate(xmin, ymin);
    GeometryFactory factory = new GeometryFactory();
    return factory.createPolygon(coordinates);
  }

  public List<Integer> getSequence(long coding) {
    int g = this.xz2Precision;
    List<Integer> list = new ArrayList<>(g);
    for (int i = 0; i < g; i++) {
      if (coding <= 0) {
        break;
      }
      long x = ((long) Math.pow(4, g - i) - 1L)/3L;
      long y = coding / x;
      list.add((int)y);
      coding = coding - 1L - x * y;
    }
    return list;
  }
}
