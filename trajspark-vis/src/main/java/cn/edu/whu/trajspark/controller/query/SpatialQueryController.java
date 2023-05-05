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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SpatialQueryController {


  private final QueryConditionService queryConditionService = new QueryConditionService();

  private final QueryService queryService = new QueryService();

  @ResponseBody
  @GetMapping(value = "/SpatialQuery/Intersect")
  public JSONObject SpatialIntersectQuery(@RequestParam(value = "dataSetName") String dataSetName,
      @RequestParam(value = "spatialWindow") String spatialWindow)
      throws ParseException, IOException {
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatIntersectSpatialCondition(
        spatialWindow);
    List<Trajectory> trajectories = queryService.executeSpatialQuery(dataSetName,
        spatialQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }

  @ResponseBody
  @GetMapping (value = "/SpatialQuery/Contained")
  public JSONObject SpatialContainedQuery(@RequestParam(value = "dataSetName") String dataSetName,
      @RequestParam(value = "spatialWindow") String spatialWindow)
      throws ParseException, IOException {
    SpatialQueryCondition spatialQueryCondition = queryConditionService.creatContainedSpatialCondition(
        spatialWindow);
    List<Trajectory> trajectories = queryService.executeSpatialQuery(dataSetName,
        spatialQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
