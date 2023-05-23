package cn.edu.whu.trajspark.example.database.store;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.HBaseStore;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.preprocess.HBaseStoreExample;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import com.fasterxml.jackson.core.JsonParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialQueryIns {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialQueryIns.class);
  static String DATASET_NAME = "DataStore_100millon";
  public static SpatialQueryCondition spatialIntersectQueryCondition;

  static {
//    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      String QUERY_WKT_INTERSECT = getQueryWindow();
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
    } catch (ParseException e) {
      e.printStackTrace();
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    Database instance = Database.getInstance();
    List<Trajectory> results = new ArrayList<>();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    List<RowKeyRange> list = XZ2IndexStrategy.getScanRanges(spatialIntersectQueryCondition);
    long startLoadTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      results = spatialQuery.executeQuery(list);
    }
    long endLoadTime = System.currentTimeMillis();
    System.out.println(results.size());
    System.out.printf("cost time: %d ms.", (endLoadTime - startLoadTime) / 10);
    instance.closeConnection();
  }

  @Test
  public void getTest() throws JsonParseException {
    String queryWindow = getQueryWindow();
    System.out.println(queryWindow);
  }

  public static String getQueryWindow() throws JsonParseException {
    String inPath = Objects.requireNonNull(
        HBaseStoreExample.class.getResource("/ioconf/testStoreConfig.json")).getPath();
    String fileStr = JSONUtil.readLocalTextFile(inPath);
//        String fileStr = "{\n" +
//        "  \"loadConfig\": {\n" +
//        "    \"@type\": \"hdfs\",\n" +
//        "    \"master\": \"local[*]\",\n" +
//        "    \"location\": \"hdfs://u0:9000/geofence/newcars/\",\n" +
//        "    \"fsDefaultName\": \"hdfs://u0:9000\",\n" +
//        "    \"fileMode\": \"multi_file\",\n" +
//        "    \"partNum\": 72,\n" +
//        "    \"splitter\": \",\"\n" +
//        "  },\n" +
//        "  \"dataConfig\": {\n" +
//        "    \"@type\": \"trajectory\",\n" +
//        "    \"trajId\": {\n" +
//        "      \"sourceName\": \"traj_id\",\n" +
//        "      \"dataType\": \"String\",\n" +
//        "      \"index\": 1\n" +
//        "    },\n" +
//        "    \"objectId\": {\n" +
//        "      \"sourceName\": \"object_id\",\n" +
//        "      \"dataType\": \"String\",\n" +
//        "      \"index\": 1\n" +
//        "    },\n" +
//        "    \"trajPointConfig\": {\n" +
//        "      \"@type\": \"traj_point\",\n" +
//        "      \"pointId\": {\n" +
//        "        \"sourceName\": \"point_id\",\n" +
//        "        \"dataType\": \"String\",\n" +
//        "        \"index\": -1\n" +
//        "      },\n" +
//        "      \"lng\": {\n" +
//        "        \"sourceName\": \"lng\",\n" +
//        "        \"dataType\": \"Double\",\n" +
//        "        \"index\": 3\n" +
//        "      },\n" +
//        "      \"lat\": {\n" +
//        "        \"sourceName\": \"lat\",\n" +
//        "        \"dataType\": \"Double\",\n" +
//        "        \"index\": 2\n" +
//        "      },\n" +
//        "      \"time\": {\n" +
//        "        \"sourceName\": \"time\",\n" +
//        "        \"dataType\": \"Date\",\n" +
//        "        \"index\": 4,\n" +
//        "        \"format\": \"yyyy-MM-dd HH:mm:ss\"\n" +
//        "      }\n" +
//        "    }\n" +
//        "  },\n" +
//        "  \"storeConfig\": {\n" +
//        "    \"@type\": \"hbase\",\n" +
//        "    \"location\": \"hdfs://u0:9000/geofence_traj/\",\n" +
//        "    \"schema\": \"POINT_BASED_TRAJECTORY\",\n" +
//        "    \"dataSetName\": \"DataStore_100millon\",\n" +
//        "    \"mainIndex\": \"XZ2\"\n" +
//        "  }\n" +
//        "}";
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    StringBuilder builder = new StringBuilder();
    try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
        TrajStore2HBase.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
              exampleConfig.getDataConfig());
      List<Trajectory> trajectories = trajRDD.take(10);
      int size = trajectories.size();
      Random random = new Random();
      int nextInt = random.nextInt(size);
      Trajectory trajectory = trajectories.get(nextInt);
      MinimumBoundingBox mbr = trajectory.getTrajectoryFeatures().getMbr();
      builder.append("POLYGON ((");
      builder.append(mbr.getMinLng() + " ");
      builder.append(mbr.getMaxLat() + ", ");
      builder.append(mbr.getMinLng() + " ");
      builder.append(mbr.getMinLat() + ", ");
      builder.append(mbr.getMaxLng() + " ");
      builder.append(mbr.getMinLat() + ", ");
      builder.append(mbr.getMaxLng() + " ");
      builder.append(mbr.getMaxLat() + ", ");
      builder.append(mbr.getMinLng() + " ");
      builder.append(mbr.getMaxLat());
      builder.append("))");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return builder.toString();
  }
}
