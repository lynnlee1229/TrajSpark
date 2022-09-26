package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.store.HDFSStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.IStoreConfig;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/21
 **/
public interface IStore extends Serializable {
  void storeTrajectory(JavaRDD<Trajectory> t);

  static IStore getStore(IStoreConfig storeConfig, IDataConfig dataConfig) {
    switch (storeConfig.getStoreType()) {
      case HDFS:
        if (storeConfig instanceof HDFSStoreConfig) {
          return new HDFSStore((HDFSStoreConfig) storeConfig);
        }

        throw new NoSuchMethodError();
//      case GEOMESA:
//        if (storeConfig instanceof GeoMesaOutputConfig) {
//          return new GeoMesaStore((GeoMesaOutputConfig)storeConfig, dataConfig);
//        }
//
//        throw new NoSuchMethodError();
      default:
        throw new NotImplementedError();
    }
  }
}
