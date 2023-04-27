package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.IDetector;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/11/3
 **/

public class StandaloneExample {

  public static void main(String[] args) throws Exception {
    String confPath =
        "/Users/lynnlee/Code/practice/TrajSpark/trajspark-examples/src/main/resources/ioconf/exampleStandaloneConfig.json";
    String fileStr = IOUtils.readFileToString(confPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);

    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        MRPreProcessExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      trajRDD.collect().forEach(System.out::println);

      // 从配置文件初始化预处理算子
      IFilter myFilter = IFilter.getFilter(exampleConfig.getFilterConfig());
      IDetector myDector = IDetector.getDector(exampleConfig.getDetectorConfig());
      ISegmenter mySegmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());

      // 执行预处理
      JavaRDD<Trajectory> filteredRDD = myFilter.filter(trajRDD);
      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(filteredRDD);
      JavaRDD<StayPoint> stayPointRDD = myDector.detect(filteredRDD);

      // 存储
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig());
      iStore.storeTrajectory(segmentedRDD);
    }
  }
}
