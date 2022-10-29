package cn.edu.whu.trajspark.example.io;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.noisefilter.IFilter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.IDetector;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class MRPreProcessExample {
  private static final Logger LOGGER = Logger.getLogger(MRPreProcessExample.class);

  public static void main(String[] args) throws IOException {
    String testPath =
        "/Users/lynnlee/Code/practice/TrajSpark/trajspark-examples/src/main/resources/ioconf/exampleConfig.json";
    String fileStr = FileSystemUtils.readFully("file:///", testPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        MRPreProcessExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      IFilter myFilter;
      ISegmenter mySegmenter;
      IDetector myDector;

//      myFilter = new BasicFilter(300, 100);
      myFilter = IFilter.getFilter(exampleConfig.getFilterConfig());
//      mySegmenter = new StayPointBasedSegmenter(500, 300, 100);
      mySegmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
//      myDector = new BasicDector(800, 900);
myDector = IDetector.getDector(exampleConfig.getDectorConfig());

      JavaRDD<Trajectory> filteredRDD = myFilter.filter(trajRDD);
//        trajRDD.collect().forEach(System.out::println);
      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(filteredRDD);
      segmentedRDD.collect().forEach(System.out::println);
//      myDector.detect(segmentedRDD).collect().forEach(System.out::println);
//      IStore iStore =
//          IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
//      iStore.storeTrajectory(segmentedRDD);
      LOGGER.info("Finished!");
    }
  }
}
