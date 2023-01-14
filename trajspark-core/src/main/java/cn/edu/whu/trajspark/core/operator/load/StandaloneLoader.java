package cn.edu.whu.trajspark.core.operator.load;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajPointConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajectoryConfig;
import cn.edu.whu.trajspark.core.conf.load.ILoadConfig;
import cn.edu.whu.trajspark.core.conf.load.StandaloneLoadConfig;
import cn.edu.whu.trajspark.core.operator.load.parser.basic.TrajectoryParser;
import java.io.File;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class StandaloneLoader implements ILoader {

  public List<Trajectory> loadTrajectory(ILoadConfig loadConfig,
                                         IDataConfig dataConfig) {
    return null;
  }

  public JavaRDD<Trajectory> loadTrajectory(SparkSession sparkSession, ILoadConfig loadConfig,
                                            IDataConfig dataConfig) {
    if (loadConfig instanceof StandaloneLoadConfig && dataConfig instanceof TrajectoryConfig) {
      StandaloneLoadConfig standaloneLoadConfig = (StandaloneLoadConfig) loadConfig;
      TrajectoryConfig trajectoryConfig = (TrajectoryConfig) dataConfig;
      switch (standaloneLoadConfig.getFileMode()) {
        case MULTI_FILE:
          return this.loadTrajectoryFromMultiFile(sparkSession, standaloneLoadConfig,
              trajectoryConfig);
        case SINGLE_FILE:
          return this.loadTrajectoryFromSingleFile(sparkSession, standaloneLoadConfig,
              trajectoryConfig);
        default:
          throw new NotSupportedException(
              "can't support fileMode " + standaloneLoadConfig.getFileMode().getMode());
      }
    } else {
      throw new RuntimeException(
          "loadConfig is not a StandAloneLoadConfig or dataConfig is not a TrajectoryConfig in configuration json file");
    }
  }

  private JavaRDD<Trajectory> loadTrajectoryFromMultiFile(SparkSession sparkSession,
                                                          StandaloneLoadConfig standaloneLoadConfig,
                                                          TrajectoryConfig trajectoryConfig) {

    int partNum = standaloneLoadConfig.getPartNum();
    return sparkSession.sparkContext().wholeTextFiles(standaloneLoadConfig.getLocation(), partNum)
        .toJavaRDD()
        .filter((s) -> {
          // 过滤空文件
          return !((String) s._2).isEmpty();
        })
        .map((s) -> {
          // 解析、映射为Trajectory
          Trajectory trajectory = TrajectoryParser.multifileParse(s._2(), trajectoryConfig,
              standaloneLoadConfig.getSplitter());
          if (trajectory != null && trajectoryConfig.getTrajId().getIndex() < 0) {
            File file = new File(s._1());
            String fileNameFull = file.getName();
            trajectory.setTrajectoryID(fileNameFull.substring(0, fileNameFull.lastIndexOf(".")));
          }
          return trajectory;
        })
        .filter(Objects::nonNull);
  }

  private JavaRDD<Trajectory> loadTrajectoryFromSingleFile(SparkSession sparkSession,
                                                           StandaloneLoadConfig standaloneLoadConfig,
                                                           TrajectoryConfig trajectoryConfig) {
    TrajPointConfig trajPointConfig = trajectoryConfig.getTrajPointConfig();
    int partNum = standaloneLoadConfig.getPartNum();
    return sparkSession.sparkContext().wholeTextFiles(standaloneLoadConfig.getLocation(), partNum)
        .toJavaRDD()
        .flatMap(
            s -> {
              return TrajectoryParser.singlefileParse(s._2, trajectoryConfig,
                      standaloneLoadConfig.getSplitter())
                  .iterator();
            }
        ).filter(Objects::nonNull);
  }
}
