package cn.edu.whu.trajspark.controller.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.util.ExampleTrajectoryUtil;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestQueryController {

  @ResponseBody
  @GetMapping(value = "/query/test")
  public JSONObject testTrajectoryToGeo() throws URISyntaxException {
    List<Trajectory> trajectories = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    List<Trajectory> trajectories1 = new ArrayList<>();
    trajectories1.add(trajectories.get(0));
    JSONObject geoJson = GeoJsonConvertor.convertGeoJson(trajectories);
    return geoJson;
  }
}
