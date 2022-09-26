package cn.edu.whu.trajspark.example.io;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class DataIOExample {
  private static final Logger LOGGER = Logger.getLogger(DataIOExample.class);

  public static void main(String[] args) throws IOException {
    String testPath =
        "/Users/lynnlee/Code/practice/TrajSpark/trajspark-examples/src/main/resources/ioconf/ioConfig.json";
//    String fileStr = FileSystemUtils.readFully(args[0], args[1]);
//    String fileStr = FileUtils.readFileToString(new File(testPath));
    String fileStr = FileSystemUtils.readFully("file:///", testPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
//    int localIndex = 2;
//    try {
//      isLocal = Boolean.parseBoolean(args[localIndex]);
//    } catch (Exception ignored) {
//    }
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        DataIOExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      IStore iStore = IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
      iStore.storeTrajectory(trajRDD);
      LOGGER.info("Finished!");
    }
  }
}
