package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.service.query.QueryConditionService;
import cn.edu.whu.trajspark.service.query.QueryService;
import cn.edu.whu.trajspark.util.ExampleTrajectoryUtil;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.io.ParseException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SpatialQueryController {


  QueryConditionService queryConditionService;

  QueryService queryService;

  @ResponseBody
  @GetMapping (value = "/query/test")
  public JSONObject testTrajectoryToGeo() throws URISyntaxException {
    List<Trajectory> trajectories = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    List<Trajectory> trajectories1 = new ArrayList<>();
    trajectories1.add(trajectories.get(0));
    JSONObject geoJson = GeoJsonConvertor.convertGeoJson(trajectories);
    return geoJson;
  }
  @ResponseBody
  @PostMapping(value = "/SpatialQuery/Intersect")
  public JSONObject SpatialIntersectQuery(@RequestParam String dataSetName,
      @RequestParam String queryWindow)
      throws ParseException, IOException {
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatIntersectSpatialCondition(
        queryWindow);
    List<Trajectory> trajectories = queryService.executeSpatialQuery(dataSetName,
        spatialQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }

  @ResponseBody
  @PostMapping(value = "/SpatialQuery/Contained")
  public JSONObject SpatialContainedQuery(@RequestParam String dataSetName,
      @RequestParam String queryWindow)
      throws ParseException, IOException {
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatContainedSpatialCondition(
        queryWindow);
    List<Trajectory> trajectories = queryService.executeSpatialQuery(dataSetName,
        spatialQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
