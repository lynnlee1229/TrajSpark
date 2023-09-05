package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.HBaseStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.HDFSStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.IStoreConfig;
import cn.edu.whu.trajspark.core.conf.store.StandaloneStoreConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/9/21
 **/
public interface IStore extends Serializable {
  void storeTrajectory(JavaRDD<Trajectory> t) throws Exception;
  void storeStayPointList(JavaRDD<List<StayPoint>> spList);
  void storeStayPointASTraj(JavaRDD<StayPoint> sp);

  static IStore getStore(IStoreConfig storeConfig) {
    switch (storeConfig.getStoreType()) {
      case HDFS:
        if (storeConfig instanceof HDFSStoreConfig) {
          return new HDFSStore((HDFSStoreConfig) storeConfig);
        }
      case STANDALONE:
        if (storeConfig instanceof StandaloneStoreConfig) {
          return new StandaloneStore((StandaloneStoreConfig) storeConfig);
        }
      case HBASE:
        if (storeConfig instanceof HBaseStoreConfig) {
          Configuration conf = HBaseConfiguration.create();
          return new HBaseStore((HBaseStoreConfig) storeConfig, conf);
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
