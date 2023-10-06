package cn.edu.whu.trajspark.core.operator.store;

import cn.edu.whu.trajspark.base.point.StayPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.core.conf.store.HBaseStoreConfig;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.mapper.TrajectoryDataMapper;
import cn.edu.whu.trajspark.database.load.mapper.datatypes.KeyFamilyQualifier;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.NotImplementedError;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 *
 * @author Xu Qi
 * @since 2023/1/3
 */
public class HBaseStore extends Configured implements IStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);
    private static Database instance;

    private final HBaseStoreConfig storeConfig;

    public HBaseStore(HBaseStoreConfig hBaseStoreConfig, Configuration conf) {
        this.storeConfig = hBaseStoreConfig;
        this.setConf(conf);
    }

    public void initDataSetTest(DataSetMeta dataSetMeta) throws IOException {
        instance = Database.getInstance();
        instance.createDataSet(dataSetMeta);
    }


    @Override
    public void storeTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws Exception {
        switch (this.storeConfig.getSchema()) {
            case POINT_BASED_TRAJECTORY:
                this.storePointBasedTrajectory(trajectoryJavaRDD);
                return;
            case POINT_BASED_TRAJECTORY_SLOWPUT:
                this.storePutPointBasedTrajectory(trajectoryJavaRDD);
                return;
            default:
                throw new NotImplementedError();
        }
    }

    public void storePutPointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws Exception {
        DataSetMeta dataSetMeta = storeConfig.getDataSetMeta();
        LOGGER.info("Starting store dataset {}", dataSetMeta.getDataSetName());
        long startLoadTime = System.currentTimeMillis();
        initDataSetTest(storeConfig.getDataSetMeta());
        IndexMeta coreIndexMeta = storeConfig.getDataSetMeta().getCoreIndexMeta();
//        IndexTable coreIndexTable = instance.getDataSet(dataSetMeta.getDataSetName())
//            .getCoreIndexTable();
        trajectoryJavaRDD.foreachPartition(item -> {
            ArrayList<Put> puts = new ArrayList<>();
            while(item.hasNext()){
                puts.add(TrajectorySerdeUtils.getPutForMainIndex(coreIndexMeta, item.next()));
            }
            instance.getTable(coreIndexMeta.getIndexTableName()).put(puts);
            puts.clear();
        });
        LOGGER.info("Successfully store to main index, meta: {}", coreIndexMeta);
        long endLoadTime = System.currentTimeMillis();
        LOGGER.info("DataSet {} load finished, cost time: {}ms.", dataSetMeta.getDataSetName(), (endLoadTime - startLoadTime));
        instance.closeConnection();
    }


    public void storePointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws Exception {
        DataSetMeta dataSetMeta = storeConfig.getDataSetMeta();
        LOGGER.info("Starting bulk load dataset {}", dataSetMeta.getDataSetName());
        long startLoadTime = System.currentTimeMillis();
        LOGGER.info("Start storing BasePointTrajectory into location : " + this.storeConfig.getLocation());
        initDataSetTest(storeConfig.getDataSetMeta());
        IndexMeta coreIndexMeta = storeConfig.getDataSetMeta().getCoreIndexMeta();
        LOGGER.info("Starting bulk load main index, meta: {}", coreIndexMeta);
        try {
            bulkLoadToMainIndexTable(trajectoryJavaRDD, coreIndexMeta);
        } catch (Exception e) {
            LOGGER.error("Failed to finish bulk load main index {}", coreIndexMeta, e);
            throw e;
        }
        LOGGER.info("Successfully bulkLoad to main index, meta: {}", coreIndexMeta);
        long endLoadTime = System.currentTimeMillis();
        LOGGER.info("DataSet {} store finished, cost time: {}ms.", dataSetMeta.getDataSetName(), (endLoadTime - startLoadTime));
        deleteHFile(storeConfig.getLocation(), getConf());
        instance.closeConnection();
    }

    public void bulkLoadToMainIndexTable(JavaRDD<Trajectory> trajectoryJavaRDD, IndexMeta mainIndexMeta) throws Exception {
        String mainTableName = mainIndexMeta.getIndexTableName();
        Job job = Job.getInstance(getConf(), "Batch Import HBase Table：" + mainTableName);
        TrajectoryDataMapper.configureHFilesOnHDFS(instance, mainTableName, job);
        Table table = instance.getTable(mainTableName);
        RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(mainTableName));
        JavaRDD<Put> putJavaRDD = trajectoryJavaRDD.map(trajectory -> TrajectoryDataMapper.mapTrajectoryToSingleRow(trajectory, mainIndexMeta));
        JavaPairRDD<KeyFamilyQualifier, KeyValue> putJavaKeyValueRDD = putJavaRDD
//            .mapToPair(
//                put -> new Tuple2<>(new ImmutableBytesWritable(put.getRow()), put))
//                .reduceByKey((key, value) -> value)
            .flatMapToPair(output -> TrajectoryDataMapper.mapPutToKeyValue(output).iterator());
//            .persist(StorageLevels.MEMORY_AND_DISK);

//        putJavaKeyValueRDD.collect();

        JavaPairRDD<ImmutableBytesWritable, KeyValue> putJavaPairRDD = putJavaKeyValueRDD
            .sortByKey(true)
            .mapToPair(cell -> new Tuple2<>(new ImmutableBytesWritable(cell._1.getRowKey()), cell._2));

        putJavaPairRDD.saveAsNewAPIHadoopFile(storeConfig.getLocation(),
            ImmutableBytesWritable.class,
            KeyValue.class, HFileOutputFormat2.class);
        
//        putJavaKeyValueRDD.unpersist();

        //  修改权限：否则可能会卡住
        FsShell shell = new FsShell(getConf());
        int setPermissionfalg = -1;
        setPermissionfalg = shell.run(new String[]{"-chmod", "-R", "777", storeConfig.getLocation()});
        if (setPermissionfalg != 0) {
            System.out.println("Set Permission failed");
            return;
        }

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
        loader.doBulkLoad(new Path(storeConfig.getLocation()), instance.getAdmin(), table, locator);
    }


    public void deleteHFile(String path, Configuration conf) throws IOException {
        Path outPath = new Path(path);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
//    Configuration conf= getConf();
//    conf.set("fs.defaultFS","hdfs://localhost:9000");
//    FileSystem fs = FileSystem.get(conf);
//    fs.delete(new Path(path),true);
        fs.close();
    }

    @Override
    public void storeStayPointList(JavaRDD<List<StayPoint>> spList) {

    }

    @Override
    public void storeStayPointASTraj(JavaRDD<StayPoint> sp) {

    }
}
