package cn.edu.whu.trajspark.controller.process;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.conf.sparkConfBuild;
import cn.edu.whu.trajspark.core.conf.process.simplifier.ISimplifierConfig;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
import cn.edu.whu.trajspark.core.operator.process.simplifier.ISimplifier;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.sun.xml.internal.bind.v2.TODO;
import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SimplifierController {

  // TODO: 2023/4/28 add simplifier rest
  @ResponseBody
  @PostMapping(value = "/Preprocess/Simplifier")
  public JSONObject getSimplifier(@RequestBody String simplifierConfig) {
    JSONObject object = JSONObject.parseObject(simplifierConfig, Feature.DisableSpecialKeyDetect);
    String trajectoryJson = object.getString("data");
    JSONObject configJson = object.fluentRemove("data");
    try {
      ExampleConfig exampleConfig = ExampleConfig.parse(configJson.toString());
      SparkSession sparkSession = sparkConfBuild.createSession("DataSimplifier", true);
      ISimplifier mySimplifier = ISimplifier.getSimplifier(
          (ISimplifierConfig) exampleConfig.getFilterConfig());
      List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(
          trajectoryJson);
      try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {
        JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
        JavaRDD<Trajectory> simplifyRDD = mySimplifier.simplify(trajRDD);
        List<Trajectory> trajectoryList = simplifyRDD.collect();
        return GeoJsonConvertor.convertGeoJson(trajectoryList);
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    return new JSONObject();
  }
}
