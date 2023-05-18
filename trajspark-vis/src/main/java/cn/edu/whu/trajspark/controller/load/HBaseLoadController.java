package cn.edu.whu.trajspark.controller.load;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.conf.sparkConfBuild;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class HBaseLoadController {

  @ResponseBody
  @PostMapping (value = "/Load/HBase",produces = "application/json;charset=UTF-8")
  public JSONObject StoreDataToHBase(@RequestBody String loadConfig)
      throws IOException {
    ExampleConfig exampleConfig = ExampleConfig.parse(loadConfig);
    SparkSession sparkSession = sparkConfBuild.createSession("HBaseLoad", true);
    ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
    JavaRDD<Trajectory> trajRDD =
        iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());
    return GeoJsonConvertor.convertGeoJson(trajRDD.collect());
  }
}
