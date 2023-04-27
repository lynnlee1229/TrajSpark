package cn.edu.whu.trajspark.controller.process;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
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
public class SegmentController {
  @ResponseBody
  @PostMapping(value = "/Preprocess/Segment")
  public JSONObject getDataSegment(@RequestBody String segmentConfig,
      @RequestBody String trajectoryJson){
    try {
      ExampleConfig exampleConfig = ExampleConfig.parse(segmentConfig);
      SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataLoader");
      ISegmenter segmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
      List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(
          trajectoryJson);
      try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
        JavaRDD<Trajectory> trajRDD = sc.parallelize(trajectories);
        JavaRDD<Trajectory> segmentRDD = segmenter.segment(trajRDD);
        List<Trajectory> trajectoryList = segmentRDD.collect();
        return GeoJsonConvertor.convertGeoJson(trajectoryList);
      }
    } catch (IOException e) {
      System.out.println(e);
    }
    return new JSONObject();
  }
}
