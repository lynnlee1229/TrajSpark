package cn.edu.whu.trajspark.core.operator.load;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.load.HDFSLoadConfig;
import cn.edu.whu.trajspark.core.conf.load.ILoadConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/14
 **/
public interface ILoader extends Serializable {
  JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig, IDataConfig dataConfig);

  static ILoader getLoader(ILoadConfig loadConfig) {
    switch (loadConfig.getInputType()) {
      case HDFS:
        if (loadConfig instanceof HDFSLoadConfig) {
          return new HDFSLoader();
        }
        throw new NoSuchMethodError();
//      case GEOMESA:
//        if (loadConfig instanceof GeoMesaInputConfig) {
//          return new GeoMesaLoader();
//        }
//        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }
}
