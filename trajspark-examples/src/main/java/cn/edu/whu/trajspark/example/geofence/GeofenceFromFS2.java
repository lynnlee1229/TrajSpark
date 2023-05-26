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

public class GeofenceFromFS2 implements Serializable {

  public static void main(String[] args) throws Exception {
    String fileStr;
//    fileStr = FileSystemUtils.readFully("hdfs://u0:9000", "/geofence/geofenceStoreConfig.json");
    if (args.length > 1) {
      String fs = args[0];
      String filePath = args[1];
      fileStr = FileSystemUtils.readFully(fs, filePath);
    } else if (args.length == 1) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = GeofenceFromFS.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleFilterConfig.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    boolean isLocal = false;
    int localIndex = 2;
    try {
      isLocal = Boolean.parseBoolean(args[localIndex]);
    } catch (Exception ignored) {
    }
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        GeofenceFromFS2.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      GeofenceConfig geofenceConfig = exampleConfig.getGeofenceConfig();
      STRTreeIndex<Geometry> treeIndex = GeofenceUtils.getIndexedGeoFence(geofenceConfig.getDefaultFs(),
          geofenceConfig.getGeofencePath(), geofenceConfig.isIndexFence());
      JavaSparkContext javaSparkContext =
          JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<TreeIndex<Geometry>> treeIndexBroadcast = javaSparkContext.broadcast(treeIndex);
      Geofence<Geometry> geofenceFunc = new Geofence<>();
      JavaRDD<Tuple2<String, String>> res =
          trajRDD.map(traj -> geofenceFunc.geofence(traj, treeIndexBroadcast.getValue()))
              .filter(Objects::nonNull);
      System.out.println(res.count());

//      // 从配置文件初始化预处理算子
//      IFilter myFilter = IFilter.getFilter(exampleConfig.getFilterConfig());
//
//      // 执行预处理
//      JavaRDD<Trajectory> filteredRDD = myFilter.filter(trajRDD);
//
//      // 存储
//      IStore iStore =
//          IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
//      iStore.storeTrajectory(filteredRDD);
    }
  }
}
