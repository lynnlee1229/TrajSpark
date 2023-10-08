package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.index.STRTreeIndex;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
import cn.edu.whu.trajspark.core.conf.analysis.geofence.GeofenceConfig;
import cn.edu.whu.trajspark.core.operator.analysis.geofence.Geofence;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Lynn Lee
 * @date 2023/04/14
 **/

public class HwTest implements Serializable {

  public static void main(String[] args) throws Exception {
    /**
     * 1.读取配置文件
     * 提供三种方式：
     * HDFS，需要指定fs和路径（例：传入2参数localhost:9000 path）
     * 本地文件系统，需要指定路径（例：传入1参数 path）
     * 资源文件，不需传参，会从项目资源文件中读取，仅本地测试使用
     */
    String fileStr;
    if (args.length > 1) {
      String fs = args[0];
      String filePath = args[1];
      fileStr = FileSystemUtils.readFully(fs, filePath);
    } else if (args.length == 1) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = HwTest.class.getClassLoader()
          .getResourceAsStream("ioconf/hw.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    // 本地测试时可以传入第三个参数，指定是否本地master运行
    boolean isLocal = false;
    int localIndex = 2;
    try {
      isLocal = Boolean.parseBoolean(args[localIndex]);
    } catch (Exception ignored) {
    }
    // 2.解析配置文件
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    // 3.初始化sparkSession
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        HwTest.class.getName(), isLocal)) {
      // 4.加载轨迹数据
      long loadStart = System.currentTimeMillis();
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      long loadEnd = System.currentTimeMillis();
    // 5.加载地理围栏数据并建立、广播索引
      long fenceStart = System.currentTimeMillis();
      GeofenceConfig geofenceConfig = exampleConfig.getGeofenceConfig();
      STRTreeIndex<Geometry> treeIndex =
          GeofenceUtils.getIndexedGeoFence(geofenceConfig.getDefaultFs(),
              geofenceConfig.getGeofencePath(), geofenceConfig.isIndexFence());
      JavaSparkContext javaSparkContext =
          JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<TreeIndex<Geometry>> treeIndexBroadcast = javaSparkContext.broadcast(treeIndex);
      // 6.空间交互关系计算
      Geofence<Geometry> geofenceFunc = new Geofence<>();
      JavaRDD<Tuple2<String, String>> res =
          trajRDD.map(traj -> geofenceFunc.geofence(traj, treeIndexBroadcast.getValue()))
              .filter(Objects::nonNull);
      // 7.收集结果
      List<Tuple2<String, String>> fencedTrajs = res.collect();
      long fenceEnd = System.currentTimeMillis();
      System.out.println("Load cost " + (loadEnd - loadStart) + "ms");
      System.out.println("GeoFence cost " + (fenceEnd - fenceStart) + "ms");
    }
  }
}
