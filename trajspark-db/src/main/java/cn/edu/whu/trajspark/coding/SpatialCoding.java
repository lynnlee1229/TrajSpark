package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.sfcurve.IndexRange;

import java.io.Serializable;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public interface SpatialCoding extends Serializable {
  long index(LineString lineString);
  long index(Envelope boundingBox);
  List<IndexRange> ranges(SpatialQueryCondition spatialQueryRange);
  Polygon getSpatialPolygon(long spatialCodingVal);
}
