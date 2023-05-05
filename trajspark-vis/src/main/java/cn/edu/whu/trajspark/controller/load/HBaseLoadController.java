package cn.edu.whu.trajspark.controller.load;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.conf.sparkConfBuild;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HBaseLoadController {

  @ResponseBody
  @PostMapping (value = "/Load/HBase",produces = "application/json;charset=UTF-8")
  public JSONObject LoadDataFromHBase(@RequestBody String loadConfig)
      throws IOException {
    ExampleConfig exampleConfig = ExampleConfig.parse(loadConfig);
    SparkSession sparkSession = sparkConfBuild.createSession("HBaseLoad", true);
    ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
    JavaRDD<Trajectory> trajRDD =
        iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());
    return GeoJsonConvertor.convertGeoJson(trajRDD.collect());
  }
}
