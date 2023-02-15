package cn.edu.whu.trajspark.core.operator.store;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_SUFFIX;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.store.HBaseStoreConfig;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.mapper.TrajectoryDataMapper;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;
import scala.Tuple2;

/**
 * @author Xu Qi
 * @since 2023/1/3
 */
public class HBaseStore extends Configured implements IStore {

  private static final Logger LOGGER = Logger.getLogger(HBaseStore.class);
  private static Database instance;

  private HBaseStoreConfig storeConfig;

  public HBaseStore(HBaseStoreConfig hBaseStoreConfig, Configuration conf) {
    this.storeConfig = hBaseStoreConfig;
    this.setConf(conf);
  }

  public void initDataSetTest(DataSetMeta dataSetMeta) throws IOException {
    instance = Database.getInstance();
    instance.openConnection();
    instance.createDataSet(dataSetMeta);
  }


  @Override
  public void storeTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws Exception {
    switch (this.storeConfig.getSchema()) {
      case POINT_BASED_TRAJECTORY:
        this.storePointBasedTrajectory(trajectoryJavaRDD);
        return;
      default:
        throw new NotImplementedError();
    }
  }

  public void storePointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws Exception {
    LOGGER.info("Storing BasePointTrajectory into location : " + this.storeConfig.getLocation());
    initDataSetTest(storeConfig.getDataSetMeta());
    String tableName = storeConfig.getDataSetMeta().getDataSetName() + DATA_TABLE_SUFFIX;
    Job job = Job.getInstance(getConf(), "Batch Import HBase Table：" + tableName);
    List<IndexMeta> indexMetaList = storeConfig.getDataSetMeta().getIndexMetaList();
    TrajectoryDataMapper.configureHFilesOnHDFS(instance, tableName, job);
    Table table = instance.getTable(tableName);
    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
    JavaRDD<Put> putJavaRDD = trajectoryJavaRDD.flatMap((trajectory -> {
      List<Put> puts = TrajectoryDataMapper.mapTrajectoryToRow(trajectory, indexMetaList);
      return puts.iterator();
    }));
    JavaPairRDD<ImmutableBytesWritable, KeyValue> putJavaPairRDD = putJavaRDD
        .mapToPair(
            put -> new Tuple2<>(new ImmutableBytesWritable(put.getRow()), put))
        .reduceByKey((key, value) -> value)
        .flatMapToPair(putpair -> TrajectoryDataMapper.mapPutToKeyValue(putpair._2).iterator())
        .sortByKey(true)
        .mapToPair(cell -> new Tuple2<>(new ImmutableBytesWritable(cell._1.getRowKey()), cell._2));
//    for (Tuple2<ImmutableBytesWritable, KeyValue> tuple2 : putJavaPairRDD.collect()) {
//      String string = Bytes.toString(tuple2._2.getQualifierArray());
//      System.out.println(string);
//    }
    putJavaPairRDD.saveAsNewAPIHadoopFile(storeConfig.getLocation(),
        ImmutableBytesWritable.class,
        KeyValue.class, HFileOutputFormat2.class);
//  修改权限：否则可能会卡住
    FsShell shell = new FsShell(getConf());
    int setPermissionfalg = -1;
    setPermissionfalg = shell.run(new String[]{"-chmod", "-R", "777", storeConfig.getLocation()});
    if (setPermissionfalg != 0) {
      System.out.println("Set Permission failed");
      return;
    }
    LOGGER.info("Successfully generated HFile");
    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
    loader.doBulkLoad(new Path(storeConfig.getLocation()), instance.getAdmin(), table, locator);
    LOGGER.info("Successfully bulkLoad to HBase");
    instance.closeConnection();
  }

  @Override
  public void storeStayPointList(JavaRDD<List<StayPoint>> spList) {

  }

  @Override
  public void storeStayPointASTraj(JavaRDD<StayPoint> sp) {

  }
}
