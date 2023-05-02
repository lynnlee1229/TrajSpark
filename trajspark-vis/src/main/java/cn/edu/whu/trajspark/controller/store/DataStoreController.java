package cn.edu.whu.trajspark.controller.store;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.conf.sparkConfBuild;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import java.io.IOException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataStoreController {


  @ResponseBody
  @PostMapping(value = "/Store", produces = "application/json;charset=UTF-8")
  public String StoreData(@RequestBody String storeConfig)
      throws IOException {
    JSONObject object = JSONObject.parseObject(storeConfig, Feature.DisableSpecialKeyDetect);
    String trajectoryJson = object.getString("data");
    JSONObject configJson = object.fluentRemove("data");
    ExampleConfig exampleConfig = ExampleConfig.parse(configJson.toString());
    List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(trajectoryJson);
    SparkSession sparkSession = sparkConfBuild.createSession("DataStore", true);
    try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {
      JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
      JavaRDD<Trajectory> featuresJavaRDD = trajRDD.map(trajectory -> {
        trajectory.getTrajectoryFeatures();
        return trajectory;
      });
      try {
        IStore iStore =
            IStore.getStore(exampleConfig.getStoreConfig());
        iStore.storeTrajectory(featuresJavaRDD);
      } catch (Exception e) {
        return ("can't save json object: " + e.toString());
      }
      return "Successfully Store Data";
    }
  }
}
