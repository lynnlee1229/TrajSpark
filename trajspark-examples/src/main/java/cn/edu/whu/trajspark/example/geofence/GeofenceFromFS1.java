package cn.edu.whu.trajspark.example.geofence;

import static cn.edu.whu.trajspark.example.geofence.GeoFenceConf.getConf1;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.index.STRTreeIndex;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
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

public class GeofenceFromFS1 implements Serializable {

  public static void main(String[] args) throws Exception {
    String fileStr;
    if (args.length > 1) {
      String fsPath = args[0];
      String filePath = args[1];
      fileStr = FileSystemUtils.readFully(fsPath, filePath);
    } else if (args.length == 1) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = GeofenceFromFS.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleFilterConfig.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    fileStr = getConf1();
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    boolean isLocal = false;
    int localIndex = 2;
    try {
      isLocal = Boolean.parseBoolean(args[localIndex]);
    } catch (Exception ignored) {
    }
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        GeofenceFromFS1.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      List<Geometry> geofenceList = GeofenceUtils.readGeoFence("/home/cluster/Data/GeoFenceOneKilo.csv");
//      List<Geometry> geofenceList = GeofenceUtils.readGeoFence("hdfs://u0:9000", "/geofence/GeoFenceOneKilo.csv");
//      List<Geometry> geofenceList = GeofenceUtils.readGeoFence("hdfs://localhost:9000", "/geofence/GeoFenceOneKilo.csv");
      TreeIndex<Geometry> treeIndex = new STRTreeIndex<Geometry>();
      treeIndex.insert(geofenceList);

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