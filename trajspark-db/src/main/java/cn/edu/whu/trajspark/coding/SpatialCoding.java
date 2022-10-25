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
public abstract class SpatialCoding implements Serializable {
  public abstract long index(LineString lineString);
  public abstract long index(Envelope boundingBox);
  public abstract List<IndexRange> ranges(SpatialQueryCondition spatialQueryRange);
  public abstract Polygon getSpatialPolygon(long spatialCodingVal);
}
