package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.basic.SpatialTemporalQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
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
public class STQueryController {
  QueryConditionService queryConditionService;

  QueryService queryService;
  @ResponseBody
  @PostMapping(value = "/STQuery/Intersect")
  public JSONObject STIntersectQuery(@RequestParam String dataSetName,
      @RequestParam String spatialWindow, @RequestParam String startWindow, @RequestParam String endWindow)
      throws ParseException, IOException {
    SpatialTemporalQueryCondition spatialTemporalQueryCondition = queryConditionService.creatIntersectSTCondition(
        spatialWindow, startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeSpatialTemporalQuery(dataSetName,
        spatialTemporalQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }

  @ResponseBody
  @PostMapping(value = "/STQuery/Contained")
  public JSONObject STContainedQuery(@RequestParam String dataSetName,
      @RequestParam String spatialWindow, @RequestParam String startWindow, @RequestParam String endWindow)
      throws ParseException, IOException {
    SpatialTemporalQueryCondition spatialTemporalQueryCondition = queryConditionService.creatContainedSTCondition(
        spatialWindow, startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeSpatialTemporalQuery(dataSetName,
        spatialTemporalQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
