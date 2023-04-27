package cn.edu.whu.trajspark.controller.process;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.IDetector;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DetectController {
  @ResponseBody
  @PostMapping(value = "/Preprocess/Detect")
  public JSONObject getDataSegment(@RequestBody String detectConfig,
      @RequestBody String trajectoryJson){
    try {
      ExampleConfig exampleConfig = ExampleConfig.parse(detectConfig);
      SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataDetect");
      IDetector detector = IDetector.getDector(exampleConfig.getDetectorConfig());
      List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(
          trajectoryJson);
      try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
        JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
        JavaRDD<StayPoint> detectRDD = detector.detect(trajRDD);
        List<StayPoint> stayPoints = detectRDD.collect();
        return GeoJsonConvertor.convertStayPointGeoJson(stayPoints);
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    return new JSONObject();
  }
}
