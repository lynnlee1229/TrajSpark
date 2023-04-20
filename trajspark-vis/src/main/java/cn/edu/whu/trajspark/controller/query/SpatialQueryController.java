package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.service.query.QueryConditionService;
import cn.edu.whu.trajspark.service.query.QueryService;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.locationtech.jts.io.ParseException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SpatialQueryController {


  QueryConditionService queryConditionService;

  QueryService queryService;


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
