package cn.edu.whu.trajspark.controller;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.service.query.QueryConditionService;
import cn.edu.whu.trajspark.service.query.QueryService;
import cn.edu.whu.trajspark.util.ExampleTrajectoryUtil;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class SpatialQueryControllerTest {
  QueryConditionService queryConditionService;

  QueryService queryService;
  static String DATASET_NAME = "xz2_intersect_query_test";
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
      "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";


  @Test
  public void testTrajectoryToGeo() throws URISyntaxException {
    List<Trajectory> trajectories = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    List<Trajectory> trajectories1 = new ArrayList<>();
    trajectories1.add(trajectories.get(0));
    JSONObject geoJson = GeoJsonConvertor.convertGeoJson(trajectories);
    System.out.println(geoJson);
  }

  @Test
  public void spatialIntersectQuery() throws ParseException, IOException {
    WKTReader wktReader = new WKTReader();
    Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatIntersectSpatialCondition(
        String.valueOf(envelopeIntersect));
    List<Trajectory> trajectories = queryService.executeSpatialQuery(DATASET_NAME,
        spatialQueryCondition);
    JSONObject geoJson = GeoJsonConvertor.convertGeoJson(trajectories);
    System.out.println(geoJson);

  }

  @Test
  public void spatialContainedQuery() throws ParseException, IOException {
    WKTReader wktReader = new WKTReader();
    Geometry envelopeContained = wktReader.read(QUERY_WKT_CONTAIN);
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatContainedSpatialCondition(
        String.valueOf(envelopeContained));
    List<Trajectory> trajectories = queryService.executeSpatialQuery(DATASET_NAME,
        spatialQueryCondition);
    JSONObject geoJson = GeoJsonConvertor.convertGeoJson(trajectories);
    System.out.println(geoJson);
  }
}