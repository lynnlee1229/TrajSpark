package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.util.FSUtils;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.InputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/11/3
 **/

public class HDFSExample2 {

  public static void main(String[] args) throws Exception {
    String fileStr;
    if (args.length == 2) {
      String fsDefaultName = args[0];
      String confPath = args[1];
      fileStr = FSUtils.readFromFS(fsDefaultName, confPath);
    } else if (args.length == 1) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = HDFSExample2.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleHDFSConfig2.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);

    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        HDFSExample2.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      ISegmenter segmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
      JavaRDD<Trajectory> segRDD = segmenter.segment(trajRDD);
//      System.out.println(trajRDD.count());
//      segRDD.collect().forEach(System.out::println);
      IStore iStore = IStore.getStore(exampleConfig.getStoreConfig());
//      iStore.storeTrajectory(segRDD);
    }
  }
}