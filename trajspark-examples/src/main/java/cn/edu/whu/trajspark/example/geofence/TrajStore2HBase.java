package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.process.segmenter.CountSegmenter;
import cn.edu.whu.trajspark.core.operator.process.segmenter.ISegmenter;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Lynn Lee
 * @since 2023/04/07
 */
public class TrajStore2HBase {
  private static final Logger LOGGER = Logger.getLogger(TrajStore2HBase.class);

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws IOException {
    String inPath = Objects.requireNonNull(
        TrajStore2HBase.class.getResource("/ioconf/geofenceStoreConfig.json")).getPath();
//    String fileStr = JSONUtil.readLocalTextFile(inPath);
    String fileStr = "{\n" +
        "  \"loadConfig\": {\n" +
        "    \"@type\": \"hdfs\",\n" +
        "    \"master\": \"local[*]\",\n" +
        "    \"location\": \"hdfs://u0:9000/geofence/newcars/\",\n" +
        "    \"fsDefaultName\": \"hdfs://u0:9000\",\n" +
        "    \"fileMode\": \"multi_file\",\n" +
        "    \"partNum\": 8,\n" +
        "    \"splitter\": \",\"\n" +
        "  },\n" +
        "  \"dataConfig\": {\n" +
        "    \"@type\": \"trajectory\",\n" +
        "    \"trajId\": {\n" +
        "      \"sourceName\": \"traj_id\",\n" +
        "      \"dataType\": \"String\",\n" +
        "      \"index\": 1\n" +
        "    },\n" +
        "    \"objectId\": {\n" +
        "      \"sourceName\": \"object_id\",\n" +
        "      \"dataType\": \"String\",\n" +
        "      \"index\": 1\n" +
        "    },\n" +
        "    \"trajPointConfig\": {\n" +
        "      \"@type\": \"traj_point\",\n" +
        "      \"pointId\": {\n" +
        "        \"sourceName\": \"point_id\",\n" +
        "        \"dataType\": \"String\",\n" +
        "        \"index\": -1\n" +
        "      },\n" +
        "      \"lng\": {\n" +
        "        \"sourceName\": \"lng\",\n" +
        "        \"dataType\": \"Double\",\n" +
        "        \"index\": 3\n" +
        "      },\n" +
        "      \"lat\": {\n" +
        "        \"sourceName\": \"lat\",\n" +
        "        \"dataType\": \"Double\",\n" +
        "        \"index\": 2\n" +
        "      },\n" +
        "      \"time\": {\n" +
        "        \"sourceName\": \"time\",\n" +
        "        \"dataType\": \"Date\",\n" +
        "        \"index\": 4,\n" +
        "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"storeConfig\": {\n" +
        "    \"@type\": \"hbase\",\n" +
        "    \"location\": \"hdfs://u0:9000/geofence_traj/\",\n" +
        "    \"schema\": \"POINT_BASED_TRAJECTORY\",\n" +
        "    \"dataSetName\": \"GEOFENCE_TRAJECTORY\",\n" +
        "    \"mainIndex\": \"OBJECT_ID_T\"\n" +
        "  }\n" +
        "}";
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = false;
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        TrajStore2HBase.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
//      trajRDD.collect().forEach(System.out::println);
      ISegmenter mySegmenter = new CountSegmenter(30, 50, 3600);
      JavaRDD<Trajectory> segmentedRDD = mySegmenter.segment(trajRDD);
      segmentedRDD.count();
      IStore iStore =
          IStore.getStore(exampleConfig.getStoreConfig());
//      iStore.storeTrajectory(segmentedRDD);
//      LOGGER.info("Finished!");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
//  @Test
//  public void testDeleteDataSet() throws IOException {
//    Database instance = Database.getInstance();
//    instance.openConnection();
//    instance.deleteDataSet("GEOFENCE_TRAJECTORY_TEST");
//  }
}
