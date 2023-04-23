package cn.edu.whu.trajspark.controller.store;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.io.ParseException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@RestController
public class HBaseStoreController {


  @ResponseBody
  @GetMapping(value = "/Store/HBase")
  public String StoreDataToHBase(@RequestParam String storeConfig, @RequestParam String trajectoryJson)
      throws IOException {
    String fileStr = JSONUtil.readLocalTextFile(storeConfig);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(trajectoryJson);
    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseStore");
    try (JavaSparkContext sc = new JavaSparkContext(sparkConf)){
      JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
      try{
        IStore iStore =
            IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
        iStore.storeTrajectory(trajRDD);
      }catch (Exception e) {
        return ("can't save json object: " + e.toString());
      }
      return  "Successfully Store Data";
    }
  }
}
