package cn.edu.whu.trajspark.example.geofence;

/**
 * @author Lynn Lee
 * @date 2023/4/14
 **/
public class GeoFenceConf {
  @SuppressWarnings("checkstyle:ConstantName")
  public static final String Conf = "{\n" +
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
  @SuppressWarnings("checkstyle:ConstantName")
  public static final String Conf1 = "{\n" +
      "  \"loadConfig\": {\n" +
      "    \"@type\": \"hdfs\",\n" +
      "    \"master\": \"local[*]\",\n" +
      "    \"location\": \"hdfs://u0:9000/geofence/newcars1/\",\n" +
      "    \"fsDefaultName\": \"hdfs://u0:9000\",\n" +
      "    \"fileMode\": \"multi_single_file\",\n" +
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

  public static String getConf() {
    return Conf;
  }

  public static String getConf1() {
    return Conf1;
  }
}
