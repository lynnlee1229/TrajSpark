package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/12/30
 */
public class HBaseStoreExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStoreExample.class);

  public static void main(String[] args) throws IOException {
    String inPath = Objects.requireNonNull(
        HBaseStoreExample.class.getResource("/ioconf/testStoreConfig.json")).getPath();
    String fileStr = JSONUtil.readLocalTextFile(inPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        HBaseStoreExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      trajRDD.collect().forEach(System.out::println);
//      ISegmenter mySegmenter;
//      mySegmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
//      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(trajRDD);
//      segmentedRDD.collect().forEach(System.out::println);
//      myDector.detect(segmentedRDD).collect().forEach(System.out::println);
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
      iStore.storeTrajectory(trajRDD);
      LOGGER.info("Finished!");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet("TRAJECTORY_TEST");
  }
}
