package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.index.STRTreeIndex;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
import cn.edu.whu.trajspark.core.operator.analysis.geofence.Geofence;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
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
 * @date 2022/11/3
 **/

public class GeofenceExample implements Serializable {

  public static void main(String[] args) throws Exception {
    String fileStr;
    if (args.length != 0) {
      String confPath = args[0];
      fileStr = IOUtils.readFileToString(confPath);
    } else {
      InputStream resourceAsStream = GeofenceExample.class.getClassLoader()
          .getResourceAsStream("ioconf/exampleFilterConfig.json");
      fileStr = IOUtils.readFileToString(resourceAsStream);
    }
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        GeofenceExample.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      trajRDD.collect().forEach(System.out::println);
      List<Geometry> geofenceList = GeofenceUtils.readBeijingDistricts(
          "/Users/lynnlee/Code/practice/TrajSpark/trajspark-examples/src/main/resources/data" +
              "/beijing_district.csv");
      TreeIndex<Geometry> treeIndex = new STRTreeIndex<Geometry>();
      treeIndex.insert(geofenceList);

      JavaSparkContext javaSparkContext =
          JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<TreeIndex<Geometry>> treeIndexBroadcast = javaSparkContext.broadcast(treeIndex);
      Geofence<Geometry> geofenceFunc = new Geofence<>();
      JavaRDD<Tuple2<String, String>> res =
          trajRDD.map(traj -> geofenceFunc.geofence(traj, treeIndexBroadcast.getValue())).filter(Objects::nonNull);
      res.collect().forEach(System.out::println);
      
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
