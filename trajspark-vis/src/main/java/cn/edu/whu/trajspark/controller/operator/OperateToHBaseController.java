package cn.edu.whu.trajspark.controller.operator;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.conf.sparkConfBuild;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.util.ExampleConfig;
import com.fasterxml.jackson.core.JsonParseException;
import java.util.Objects;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OperateToHBaseController {

  @ResponseBody
  @PostMapping(value = "/Operate", produces = "application/json;charset=UTF-8")
  public String processTrajData(@RequestBody String config) throws JsonParseException {
    ExampleConfig exampleConfig = ExampleConfig.parse(config);
    SparkSession sparkSession = sparkConfBuild.createSession("DataOperate", true);
    ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
    JavaRDD<Trajectory> trajRDD =
        iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
            exampleConfig.getDataConfig());

    IFilter myFilter = IFilter.getFilter(exampleConfig.getFilterConfig());
    ISegmenter segmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
    IStore iStore =
        IStore.getStore(exampleConfig.getStoreConfig());

    try {
      JavaRDD<Trajectory> filteredRDD = myFilter.filter(trajRDD);
      JavaRDD<Trajectory> segmentRDD = segmenter.segment(filteredRDD);

      JavaRDD<Trajectory> featuresJavaRDD = segmentRDD.map(trajectory -> {
        trajectory.getTrajectoryFeatures();
        return trajectory;
      });

      iStore.storeTrajectory(featuresJavaRDD);
    } catch (Exception e) {
      return ("can't save json object: " + e.toString());
    }
    return "Successfully Process Data";
  }

}
