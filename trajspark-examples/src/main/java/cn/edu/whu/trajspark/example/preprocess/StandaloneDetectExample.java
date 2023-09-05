package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.process.staypointdetector.IDetector;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/11/3
 **/

public class StandaloneDetectExample {

  public static void main(String[] args) throws IOException {
    String fileStr;
    if (args.length != 0) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = StandaloneDetectExample.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleDetectConfig.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);

    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        StandaloneDetectExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());

      // 从配置文件初始化预处理算子
      ISegmenter mySegmenter = ISegmenter.getSegmenter(exampleConfig.getSegmenterConfig());
      IDetector myDector = IDetector.getDector(exampleConfig.getDetectorConfig());

      // 执行预处理
      JavaRDD<Trajectory> segmentedTraj = mySegmenter.segment(trajRDD);
      JavaRDD<List<StayPoint>> stayPointList = (JavaRDD<List<StayPoint>>) segmentedTraj.map(myDector::detectFunction);
      JavaRDD<StayPoint> detectRDD = myDector.detect(segmentedTraj);
      // 存储
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig());
      iStore.storeStayPointList(stayPointList);
//      iStore.storeStayPointASTraj(detectRDD);
    }
  }
}
