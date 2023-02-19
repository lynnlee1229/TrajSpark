package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/19
 */
public class BulkloadTest {
  static DataSetMeta dataSetMeta;
  static String database_name = "bulkLoadTest3";

  static {
    List<IndexMeta> list = new LinkedList<>();
    IndexMeta coreIndexMata = new IndexMeta(true, new XZ2IndexStrategy(), database_name, "default");
    list.add(coreIndexMata);
    // list.add(new IndexMeta(true, new XZ2TIndexStrategy(), database_name, coreIndexMata, "default"));
    // list.add(new IndexMeta(true, new TXZ2IndexStrategy(), database_name, coreIndexMata, "default"));
    list.add(new IndexMeta(false, new IDTIndexStrategy(), database_name, coreIndexMata, "default"));
    dataSetMeta = new DataSetMeta(database_name, list);
  }

  // 确保hbase-site.xml, core-site.xml, hdfs-site.xml在class path中。
  // hdfs dfs -put traj/hdfs_traj_example.txt /data/
  @Test
  public void testBulkLoad() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String inPath = "/data/hdfs_traj_example.txt";
    String output = "/tmp/";
    Database.getInstance().createDataSet(dataSetMeta);
    TrajectoryDataDriver trajectoryDataDriver = new TrajectoryDataDriver();
    trajectoryDataDriver.setConf(conf);
    trajectoryDataDriver.bulkLoad(new Parser(), inPath, output, dataSetMeta);
  }

  private static class Parser implements TextTrajParser {
    @Override
    public Trajectory parse(String line) throws ParseException {
      String[] strs = line.split("\\|");
      String carNo = strs[0];
      Trajectory t = new Trajectory(
          getTrajectoryID(line),
          carNo,
          toTrajPointList(strs[2], strs[1]));
      return t;
    }

    private String getTrajectoryID(String line) {
      String[] timestampStrs = line.split("\\|")[1].split(",");
      return timestampStrs[0];
    }

    private List<TrajPoint> toTrajPointList(String lineWKT, String timestampStr) throws ParseException {
      List<TrajPoint> list = new LinkedList<>();
      String[] timestampStrs = timestampStr.split(",");
      WKTReader wktReader = new WKTReader();
      Coordinate[] coordinates = ((LineString) wktReader.read(lineWKT)).getCoordinates();
      for (int i = 0; i < coordinates.length; i++) {
        Instant instant = Instant.ofEpochSecond(Long.parseLong(timestampStrs[i]) / 1000L);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("Asia/Shanghai"));
        double lng = coordinates[i].y;
        double lat = coordinates[i].x;
        TrajPoint trajPoint = new TrajPoint(zonedDateTime, lng, lat);
        list.add(trajPoint);
      }
      return list;
    }
  }
}
