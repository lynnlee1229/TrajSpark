package cn.edu.whu.trajspark.example.database.store;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.HBaseStore;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import java.io.IOException;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialQueryIns {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialQueryIns.class);
  static String DATASET_NAME = "DataStore_100millon";
  public static SpatialQueryCondition spatialIntersectQueryCondition;
  public static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
      "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      Geometry envelopeContained = wktReader.read(QUERY_WKT_CONTAIN);
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition = new SpatialQueryCondition(envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    long startLoadTime = System.currentTimeMillis();
    Database instance = Database.getInstance();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    long endLoadTime = System.currentTimeMillis();
    LOGGER.info("DataSet {} load finished, cost time: {}ms.", DATASET_NAME, (endLoadTime - startLoadTime));
    instance.closeConnection();
  }

}
