package cn.edu.whu.trajspark.example.database.store;

import cn.edu.whu.trajspark.base.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.preprocess.HBaseStoreExample;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.query.basic.SpatialQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition.SpatialQueryType;
import cn.edu.whu.trajspark.query.coprocessor.autogenerated.QueryCondition.QueryRequest;
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
  static String DATASET_NAME = "DataStore_100millon_1";
  public static SpatialQueryCondition spatialIntersectQueryCondition;

  static {
//    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      String inPath = "/usr/local/city_data/geofence/geofence.txt";
//      String inPath = Objects.requireNonNull(
//          HBaseStoreExample.class.getResource("/ioconf/geofence.txt")).getPath();
      String QUERY_WKT_INTERSECT = getGeofence(inPath);
//      System.out.println(QUERY_WKT_INTERSECT);
//      String QUERY_WKT_INTERSECT = "POLYGON ((116.327335 39.591614, 116.327335 39.135463, 116.514668 39.135463, 116.514668 39.591614, 116.327335 39.591614))";
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryType.INTERSECT);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    Database instance = Database.getInstance();
    List<Trajectory> results = new ArrayList<>();
    IndexTable coreIndexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery = new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    List<RowKeyRange> list = XZ2IndexStrategy.getScanRanges(spatialIntersectQueryCondition);
    QueryRequest query = spatialQuery.getQuery(list);
    long startLoadTime = System.currentTimeMillis();
    results = spatialQuery.executeQueryScan(coreIndexTable, query, 50);
    long endLoadTime = System.currentTimeMillis();
    long timecost = (endLoadTime - startLoadTime) / 50;
    System.out.println();
    System.out.printf("Trajectory num:  %d .", results.size());
    System.out.printf("cost time: %d ms.", timecost);
    System.out.println();
    instance.closeConnection();
  }

  @Test
  public void getTest() throws JsonParseException {
    List<String> queryWindow = getQueryWindow();
    for (String s : queryWindow) {
      System.out.println(s);
    }
  }

  public static String getGeofence(String inPath){
    String fileStr = JSONUtil.readLocalTextFileLine(inPath);
    String[] split = fileStr.split("\n");
    int size = split.length;
    Random random = new Random();
    int nextInt = random.nextInt(size);
    return split[nextInt];
  }


  public static List<String> getQueryWindow() throws JsonParseException {
    String inPath = Objects.requireNonNull(
        HBaseStoreExample.class.getResource("/ioconf/testStoreConfig.json")).getPath();
    String fileStr = JSONUtil.readLocalTextFile(inPath);
//        String fileStr = "{\n" +
//        "  \"loadConfig\": {\n" +
//        "    \"@type\": \"hdfs\",\n" +
//        "    \"master\": \"local[*]\",\n" +
//        "    \"location\": \"hdfs://master:9000/geofence/newcars2/\",\n" +
//        "    \"fsDefaultName\": \"hdfs://master:9000\",\n" +
//        "    \"fileMode\": \"multi_file\",\n" +
//        "    \"partNum\": 8,\n" +
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
//        "    \"location\": \"hdfs://master:9000/geofence_traj/\",\n" +
//        "    \"schema\": \"POINT_BASED_TRAJECTORY\",\n" +
//        "    \"dataSetName\": \"DataStore_100millon\",\n" +
//        "    \"mainIndex\": \"XZ2\"\n" +
//        "  }\n" +
//        "}";
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    ArrayList<String> strings = new ArrayList<>();

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
//      Trajectory trajectory = trajectories.get(nextInt);
      for (Trajectory trajectory : trajectories) {
        StringBuilder builder = new StringBuilder();
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
        strings.add(builder.toString());
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return strings;
  }
}
