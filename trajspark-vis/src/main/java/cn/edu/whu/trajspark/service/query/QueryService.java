package cn.edu.whu.trajspark.service.query;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.query.advanced.PKNNQuery;
import cn.edu.whu.trajspark.query.basic.IDTemporalQuery;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.basic.SpatialTemporalQuery;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.io.IOException;
import java.util.List;
import org.springframework.stereotype.Service;


public class QueryService {

  public QueryService() {
  }

  public List<Trajectory> executeSpatialQuery(String dataSetName,
      SpatialQueryCondition spatialQueryCondition) throws IOException {
    Database instance = Database.getInstance();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(dataSetName),
        spatialQueryCondition);
    return spatialQuery.executeQuery();
  }

  public List<Trajectory> executeTemporalQuery(String dataSetName,
      TemporalQueryCondition temporalQueryCondition, IDQueryCondition idQueryCondition)
      throws IOException {
    Database instance = Database.getInstance();
    IDTemporalQuery idTemporalQuery = new IDTemporalQuery(instance.getDataSet(dataSetName),
        temporalQueryCondition, idQueryCondition);
    return idTemporalQuery.executeQuery();
  }

  public List<Trajectory> executeSpatialTemporalQuery(String dataSetName,
      SpatialTemporalQueryCondition spatialTemporalQueryCondition)
      throws IOException {
    Database instance = Database.getInstance();
    SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(
        instance.getDataSet(dataSetName), spatialTemporalQueryCondition);
    return spatialTemporalQuery.executeQuery();
  }

  public List<Trajectory> executeKNNQuery(String dataSetName, int num, BasePoint basePoint,
      TemporalQueryCondition temporalQueryCondition, double maxDistKM) throws IOException {
    Database instance = Database.getInstance();
    PKNNQuery pknnQuery = new PKNNQuery(instance.getDataSet(dataSetName), num, basePoint,
        temporalQueryCondition, maxDistKM);
    return pknnQuery.execute();
  }

}
