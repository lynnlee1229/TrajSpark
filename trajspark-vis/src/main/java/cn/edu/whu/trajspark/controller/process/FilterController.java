package cn.edu.whu.trajspark.controller.process;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FilterController {

  @ResponseBody
  @PostMapping(value = "/Preprocess/Filter")
  public JSONObject getDataFilter(@RequestBody String filterConfig,
      @RequestBody String trajectoryJson) {
    try {
      ExampleConfig exampleConfig = ExampleConfig.parse(filterConfig);
      SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataLoader");
      IFilter myFilter = IFilter.getFilter(exampleConfig.getFilterConfig());
      List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(
          trajectoryJson);
      try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
        JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
        JavaRDD<Trajectory> filteredRDD = myFilter.filter(trajRDD);
        List<Trajectory> trajectoryList = filteredRDD.collect();
        return GeoJsonConvertor.convertGeoJson(trajectoryList);
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    return new JSONObject();
  }
}
