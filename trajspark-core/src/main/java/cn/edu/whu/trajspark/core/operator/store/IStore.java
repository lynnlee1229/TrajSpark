package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.core.common.point.StayPoint;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.store.HDFSStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.IStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.StandaloneStoreConfig;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/21
 **/
public interface IStore extends Serializable {
  void storeTrajectory(JavaRDD<Trajectory> t);
  void storeStayPointList(JavaRDD<List<StayPoint>> spList);
  void storeStayPointASTraj(JavaRDD<StayPoint> sp);

  static IStore getStore(IStoreConfig storeConfig, IDataConfig dataConfig) {
    switch (storeConfig.getStoreType()) {
      case HDFS:
        if (storeConfig instanceof HDFSStoreConfig) {
          return new HDFSStore((HDFSStoreConfig) storeConfig);
        }
      case STANDALONE:
        if (storeConfig instanceof StandaloneStoreConfig) {
          return new StandaloneStore((StandaloneStoreConfig) storeConfig);
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
