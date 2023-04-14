package cn.edu.whu.trajspark.example.geofence;

import static cn.edu.whu.trajspark.example.geofence.GeoFenceConf.getConf;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.common.index.STRTreeIndex;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
import cn.edu.whu.trajspark.core.operator.analysis.geofence.Geofence;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.segmenter.CountSegmenter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
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

public class GeofenceFromFS implements Serializable {

  public static void main(String[] args) throws Exception {
    String fileStr;
//    if (args.length != 0) {
//      String confPath = args[0];
//      fileStr = IOUtils.readFileToString(confPath);
//    } else {
//      InputStream resourceAsStream = GeofenceFromFS.class.getClassLoader()
//          .getResourceAsStream("ioconf/exampleFilterConfig.json");
//      fileStr = IOUtils.readFileToString(resourceAsStream);
//    }
    fileStr = getConf();
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    boolean isLocal = false;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        GeofenceFromFS.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      ISegmenter mySegmenter = new CountSegmenter();
      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(trajRDD);
      List<Geometry> geofenceList = GeofenceUtils.readGeoFence("/home/cluster/Data/GeoFenceOneKilo.csv");
      TreeIndex<Geometry> treeIndex = new STRTreeIndex<Geometry>();
      treeIndex.insert(geofenceList);

      JavaSparkContext javaSparkContext =
          JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<TreeIndex<Geometry>> treeIndexBroadcast = javaSparkContext.broadcast(treeIndex);
      Geofence<Geometry> geofenceFunc = new Geofence<>();
      JavaRDD<Tuple2<String, String>> res =
          segmentedRDD.map(traj -> geofenceFunc.geofence(traj, treeIndexBroadcast.getValue())).filter(Objects::nonNull);
      res.count();
      
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
