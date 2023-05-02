package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import cn.edu.whu.trajspark.service.query.QueryConditionService;
import cn.edu.whu.trajspark.service.query.QueryService;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KNNQueryController {

  private final QueryConditionService queryConditionService = new QueryConditionService();

  private final QueryService queryService = new QueryService();

  @ResponseBody
  @GetMapping(value = "/KNNQuery")
  public JSONObject SpatialIntersectQuery(@RequestParam(value = "dataSetName") String dataSetName,
      @RequestParam(value = "Center_lng") String center_lng,
      @RequestParam(value = "Center_lat") String center_lat,
      @RequestParam(value = "startWindow") String startWindow,
      @RequestParam(value = "endWindow") String endWindow,
      @RequestParam(value = "num") int pointNum,
      @RequestParam(value = "maxDistKM") double maxDistKM
      ) throws IOException {
    BasePoint basePoint = new BasePoint(Double.parseDouble(center_lng), Double.parseDouble(center_lat));
    TemporalQueryCondition temporalQueryCondition = queryConditionService.creatContainedTemporalCondition(
        startWindow, endWindow);
    List<Trajectory> trajectories = queryService.executeKNNQuery(dataSetName, pointNum, basePoint,
        temporalQueryCondition, maxDistKM);
    return GeoJsonConvertor.convertGeoJson(trajectories);
  }
}
