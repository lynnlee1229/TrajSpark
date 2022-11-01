package cn.edu.whu.trajspark.example.database.xz2;

import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.query.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;

import static cn.edu.whu.trajspark.example.database.xz2.DataSetManage.DATASET_NAME;

/**
 * @author Haocheng Wang
 * Created on 2022/11/1
 */
public class DataSetQuery {

  static String QUERY_WKT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";

  public static void simpleQuery() throws IOException, ParseException {
    // 1. get database instance
    Database instance = Database.getInstance();
    instance.openConnection();
    // 2. create SpatialQuery
    SpatialQueryCondition spatialQueryCondition = new SpatialQueryCondition(wkt2Envelope(QUERY_WKT),
        SpatialQueryCondition.SpatialQueryType.INTERSECT);
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialQueryCondition);
    // 3. execute query
    System.out.println(spatialQuery.executeQuery());
  }

  private static Envelope wkt2Envelope(String wkt) throws ParseException {
    WKTReader wktReader = new WKTReader();
    return wktReader.read(wkt).getEnvelopeInternal();
  }
}
