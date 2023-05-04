package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.simplifier.ISimplifier;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/11/3
 **/

public class StandaloneSimplifyExample {

  public static void main(String[] args) throws IOException {
    String fileStr;
    if (args.length != 0) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = StandaloneSimplifyExample.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleSimplifierConfig.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);

    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        StandaloneSimplifyExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());

      // 从配置文件初始化预处理算子
      ISimplifier mySimplifier = ISimplifier.getSimplifier(exampleConfig.getSimplifierConfig());
      // 执行预处理
      JavaRDD<Trajectory> simplifiedTraj = mySimplifier.simplify(trajRDD);
      // 存储
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig());
      iStore.storeTrajectory(simplifiedTraj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
