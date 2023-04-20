package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.basic.IDTemporalQuery;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.service.query.QueryConditionService;
import cn.edu.whu.trajspark.service.query.QueryService;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IDTemporalQueryController {

  QueryConditionService queryConditionService;

  QueryService queryService;

  @ResponseBody
  @PostMapping(value = "/IDTemporalQuery/Contained")
  public JSONObject IDTemporalContainedQuery(@RequestParam String dataSetName,
      @RequestParam String startWindow, @RequestParam String endWindow, @RequestParam String id)
      throws IOException {
    IDQueryCondition idQueryCondition = new IDQueryCondition(id);
    TemporalQueryCondition temporalIntersectCondition = queryConditionService.creatContainedTemporalCondition(
        startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeTemporalQuery(dataSetName,
        temporalIntersectCondition,
        idQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }

  @ResponseBody
  @PostMapping(value = "/IDTemporalQuery/Intersect")
  public JSONObject IDTemporalIntersectQuery(@RequestParam String dataSetName,
      @RequestParam String startWindow, @RequestParam String endWindow, @RequestParam String id)
      throws IOException {
    IDQueryCondition idQueryCondition = new IDQueryCondition(id);
    TemporalQueryCondition temporalIntersectCondition = queryConditionService.creatIntersectTemporalCondition(
        startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeTemporalQuery(dataSetName,
        temporalIntersectCondition,
        idQueryCondition);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
