package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.segmenter.CountSegmenter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import java.io.IOException;
import java.util.Objects;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @since 2023/04/07
 */
public class TrajStore2HBase {
  private static final Logger LOGGER = Logger.getLogger(TrajStore2HBase.class);

  public static void main(String[] args) throws IOException {
    String inPath = Objects.requireNonNull(
        TrajStore2HBase.class.getResource("/ioconf/geofenceStoreConfig.json")).getPath();
    String fileStr = JSONUtil.readLocalTextFile(inPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        TrajStore2HBase.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      trajRDD.collect().forEach(System.out::println);
      ISegmenter mySegmenter = new CountSegmenter();
      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(trajRDD);
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig(), exampleConfig.getDataConfig());
      iStore.storeTrajectory(segmentedRDD);
      LOGGER.info("Finished!");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
//  @Test
//  public void testDeleteDataSet() throws IOException {
//    Database instance = Database.getInstance();
//    instance.openConnection();
//    instance.deleteDataSet("TRAJECTORY_GEOFENCE");
//  }
}
