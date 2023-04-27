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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class STQueryController {

  private final QueryConditionService queryConditionService = new QueryConditionService();

  private final QueryService queryService = new QueryService();

  @ResponseBody
  @GetMapping(value = "/STQuery/Intersect")
  public JSONObject STIntersectQuery(@RequestParam(value = "dataSetName") String dataSetName,
      @RequestParam(value = "spatialWindow") String spatialWindow,
      @RequestParam(value = "startWindow") String startWindow,
      @RequestParam(value = "endWindow") String endWindow)
      throws ParseException, IOException {
    SpatialTemporalQueryCondition spatialTemporalQueryCondition = queryConditionService.creatIntersectSTCondition(
        spatialWindow, startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeSpatialTemporalQuery(dataSetName,
        spatialTemporalQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }

  @ResponseBody
  @GetMapping(value = "/STQuery/Contained")
  public JSONObject STContainedQuery(@RequestParam(value = "dataSetName") String dataSetName,
      @RequestParam(value = "spatialWindow") String spatialWindow,
      @RequestParam(value = "startWindow") String startWindow,
      @RequestParam(value = "endWindow") String endWindow)
      throws ParseException, IOException {
    SpatialTemporalQueryCondition spatialTemporalQueryCondition = queryConditionService.creatContainedSTCondition(
        spatialWindow, startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeSpatialTemporalQuery(dataSetName,
        spatialTemporalQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
