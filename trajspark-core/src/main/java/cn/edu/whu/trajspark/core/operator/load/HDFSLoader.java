package cn.edu.whu.trajspark.core.operator.load;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.parser.basic.TrajPointParser;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajPointConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajectoryConfig;
import cn.edu.whu.trajspark.core.conf.load.HDFSLoadConfig;
import cn.edu.whu.trajspark.core.conf.load.ILoadConfig;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.ws.rs.NotSupportedException;
import java.util.*;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class HDFSLoader implements ILoader {
  private static final Logger LOGGER = Logger.getLogger(HDFSLoader.class);

  public JavaRDD<Trajectory> loadTrajectory(SparkSession sparkSession, ILoadConfig loadConfig,
                                            IDataConfig dataConfig) {
    if (loadConfig instanceof HDFSLoadConfig && dataConfig instanceof TrajectoryConfig) {
      HDFSLoadConfig hdfsLoadConfig = (HDFSLoadConfig) loadConfig;
      TrajectoryConfig trajectoryConfig = (TrajectoryConfig) dataConfig;
      switch (hdfsLoadConfig.getFileMode()) {
        case MULTI_FILE:
          return this.loadTrajectoryFromMultiFile(sparkSession, hdfsLoadConfig, trajectoryConfig);
        case SINGLE_FILE:
          return this.loadTrajectoryFromSingleFile(sparkSession, hdfsLoadConfig, trajectoryConfig);
        default:
          throw new NotSupportedException(
              "can't support fileMode " + hdfsLoadConfig.getFileMode().getMode());
      }
    } else {
      LOGGER.error(
          "This loadConfig is not a HDFSLoadConfig or this dataConfig is not a TrajectoryConfig！Please check your config file");
      throw new RuntimeException(
          "loadConfig is not a HDFSLoadConfig or dataConfig is not a TrajectoryConfig in configuration json file");
    }
  }

  private JavaRDD<Trajectory> loadTrajectoryFromMultiFile(SparkSession sparkSession,
                                                          HDFSLoadConfig hdfsLoadConfig,
                                                          TrajectoryConfig trajectoryConfig) {
    LOGGER.info(
        "Loading trajectories from multi_files in folder: " + hdfsLoadConfig.getLocation());
    int partNum = hdfsLoadConfig.getPartNum();
    return sparkSession.sparkContext().wholeTextFiles(hdfsLoadConfig.getLocation(), partNum)
        .toJavaRDD().filter((s) -> {
          return !((String) s._2).isEmpty();
        }).map((s) -> {
          String[] points = ((String) s._2).split("\n");
          List<TrajPoint> trajPoints = new ArrayList(points.length);

          String[] strings;
          String pStr;
          try {
            strings = points;
            int n = points.length;

            for (int i = 0; i < n; ++i) {
              pStr = strings[i];
              TrajPoint point =
                  TrajPointParser.parse(pStr, trajectoryConfig.getTrajPointConfig(),
                      hdfsLoadConfig.getSplitter());
              trajPoints.add(point);
            }
          } catch (Exception var9) {
          }

          strings = ((String) s._1).split("/");
          String name = strings[strings.length - 1];
          String trajId, objectId;
          if (trajectoryConfig.getTrajId() != null) {
            trajId = points[0].split(hdfsLoadConfig.getSplitter())[trajectoryConfig.getTrajId()
                .getIndex()];
          } else {
            trajId = name.substring(0, name.lastIndexOf("."));
          }
          if (trajectoryConfig.getObjectId() != null) {
            objectId = points[0].split(hdfsLoadConfig.getSplitter())[trajectoryConfig.getObjectId()
                .getIndex()];
          } else {
            objectId = name.substring(0, name.lastIndexOf("."));
          }
          return trajPoints.isEmpty() ? null : new Trajectory(trajId, objectId, trajPoints);
        }).filter(Objects::nonNull);
  }

  private JavaRDD<Trajectory> loadTrajectoryFromSingleFile(SparkSession sparkSession,
                                                           HDFSLoadConfig hdfsLoadConfig,
                                                           TrajectoryConfig trajectoryConfig) {
    TrajPointConfig trajPointConfig = trajectoryConfig.getTrajPointConfig();
    LOGGER.info("Loading trajectories from single file : " + hdfsLoadConfig.getLocation());
    int partNum = hdfsLoadConfig.getPartNum();
    JavaPairRDD<Tuple2<String, String>, String> javaPairRDD =
        sparkSession.sparkContext().textFile(hdfsLoadConfig.getLocation(), partNum).toJavaRDD()
            .mapToPair((line) -> {
              int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
              int trajIdIndex = trajectoryConfig.getTrajId().getIndex();
              String[] lineArr = line.split(hdfsLoadConfig.getSplitter());
              return new Tuple2(new Tuple2(lineArr[objectIdIndex], lineArr[trajIdIndex]), line);
            });
    return javaPairRDD.groupByKey().map((groupLines) -> {
      String objectId = (String) ((Tuple2) groupLines._1)._1;
      String trajectoryId = (String) ((Tuple2) groupLines._1)._2;
      Iterator<String> iterator = ((Iterable) groupLines._2).iterator();
      List<TrajPoint> trajPoints = new ArrayList();

      while (iterator.hasNext()) {
        try {
          TrajPoint point = TrajPointParser.parse((String) iterator.next(), trajPointConfig,
              hdfsLoadConfig.getSplitter());
          trajPoints.add(point);
        } catch (Exception var7) {
        }
      }

      if (!trajPoints.isEmpty()) {
        trajPoints.sort((o1, o2) -> {
          return (int) (o1.getTimestamp().toEpochSecond() - o2.getTimestamp().toEpochSecond());
        });
        return new Trajectory(trajectoryId, objectId, trajPoints);
      } else {
        return null;
      }
    }).filter(Objects::nonNull);
  }
}
