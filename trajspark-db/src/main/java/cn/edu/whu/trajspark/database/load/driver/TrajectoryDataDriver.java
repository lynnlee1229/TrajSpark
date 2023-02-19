package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import cn.edu.whu.trajspark.database.load.mapper.CoreIndexTableMapper;
import cn.edu.whu.trajspark.database.load.mapper.TextMapper;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TrajectoryDataDriver extends Configured {

  private static Logger logger = LoggerFactory.getLogger(TrajectoryDataDriver.class);


  public static final String PROCESS_INPUT_CONF_KEY = "import.process.input.path";

  /**
   * MapReduce Bulkload 输出文件的路径
   */
  public static final String FILE_OUTPUT_CONF_KEY = "import.file.output.path";

  static Database instance;

  private void setUpConfiguration(String inPath, String outPath) {
    Configuration conf = getConf();
    conf.set(PROCESS_INPUT_CONF_KEY, inPath);
    conf.set(FILE_OUTPUT_CONF_KEY, outPath);
  }

  /**
   * 将文本文件中的数据bulk load到所有的主索引表中
   */
  private void doBulkLoadMain(Class<? extends Mapper> cls, Configuration conf, IndexTable indexTable) throws IOException {
    Path inPath = new Path(conf.get(PROCESS_INPUT_CONF_KEY));
    Path outPath = new Path(conf.get(FILE_OUTPUT_CONF_KEY));
    String tableName = indexTable.getIndexMeta().getIndexTableName();
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + tableName);
    job.setJarByClass(TrajectoryDataDriver.class);
    FileInputFormat.setInputPaths(job, inPath);
    FileSystem fs = outPath.getFileSystem(conf);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }
    FileOutputFormat.setOutputPath(job, outPath);

    //Configure Map related content
    job.setMapperClass(cls);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    //配置Reduce
    job.setNumReduceTasks(1);
    job.setReducerClass(PutSortReducer.class);

    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
    try (Admin admin = instance.getAdmin(); Table table = instance.getTable(tableName)) {
      HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
      job.waitForCompletion(true);
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(outPath, admin, table, locator);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * 将核心索引表中的数据bulk load到所有的辅助索引表中
   */
  private int doBulkLoadSecondary(Configuration conf, IndexTable secondaryTable) throws IOException {
    Path outPath = new Path(conf.get(FILE_OUTPUT_CONF_KEY));
    String inputTableName = secondaryTable.getIndexMeta().getCoreIndexTableName();
    String outTableName = secondaryTable.getIndexMeta().getIndexTableName();
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + outTableName);
    job.setJarByClass(TrajectoryDataDriver.class);
    // 设置MapReduce任务输出的路径
    FileSystem fs = outPath.getFileSystem(conf);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }
    FileOutputFormat.setOutputPath(job, outPath);

    // 配置Map算子
    CoreIndexTableMapper.setSecondaryTable(secondaryTable);
    TableMapReduceUtil.initTableMapperJob(inputTableName,
        buildCoreIndexScan(),
        CoreIndexTableMapper.class,
        ImmutableBytesWritable.class,
        Put.class,
        job);

    // 配置Reduce算子
    job.setNumReduceTasks(1);
    job.setReducerClass(PutSortReducer.class);

    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(outTableName));
    try (Admin admin = instance.getAdmin(); Table table = instance.getTable(outTableName)) {
      HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
      if (!job.waitForCompletion(true)) {
        return -1;
      }
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(outPath, admin, table, locator);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return 0;
  }

  private Scan buildCoreIndexScan() {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(DBConstants.INDEX_TABLE_CF));
    return scan;
  }

  private void mainIndexBulkLoad(TextTrajParser parser, String inPath, String outPath, DataSetMeta dataSetMeta) throws Exception {
    // 多个dataset meta，先处理其中的主索引，后处理其中的辅助索引
    List<IndexMeta> indexMetaList = dataSetMeta.getIndexMetaList();
    for (IndexMeta im : indexMetaList) {
      if (im.isMainIndex()) {
        long startLoadTime = System.currentTimeMillis();
        logger.info("Starting bulk load main index, meta: {}", im);
        IndexTable indexTable = new IndexTable(im);
        setUpConfiguration(inPath, outPath);
        try {
          TextMapper.config(indexTable, parser::parse);
          doBulkLoadMain(TextMapper.class, getConf(), indexTable);
        } catch (Exception e) {
          logger.error("Failed to finish bulk load main index {}", im, e);
          throw e;
        }
        long endLoadTime = System.currentTimeMillis();
        logger.info("Index {} load finished, cost time: {}ms.", im, (endLoadTime - startLoadTime));
      }
    }
  }

  private void secondaryIndexBulkLoad(String outPath, DataSetMeta dataSetMeta) throws Exception {
    for (IndexMeta im : dataSetMeta.getIndexMetaList()) {
      if (!im.isMainIndex()) {
        long startLoadTime = System.currentTimeMillis();
        logger.info("Starting bulk load secondary index, meta: {}", im);
        Configuration conf = getConf();
        conf.set(FILE_OUTPUT_CONF_KEY, outPath);
        try {
          doBulkLoadSecondary(getConf(), new IndexTable(im));
        } catch (Exception e) {
          logger.error("Failed to finish bulk load secondary index {}", im, e);
          throw e;
        }
        long endLoadTime = System.currentTimeMillis();
        logger.info("Index {} load finished, cost time: {}ms.", im, (endLoadTime - startLoadTime));
      }
    }
  }

  /**
   * 将文本形式的轨迹数据以MapReduce BulkLoad的形式导入至HBase表中。
   *
   * @param parser 将文本解析为Trajectory对象的实现类
   * @param inPath 输入文件路径，该文件应位于HDFS上
   * @param outPath MapReduce BulkLoad过程中产生的中间文件路径，该文件应位于HDFS上
   * @param dataSetMeta 目标数据集的元信息
   * @throws Exception BulkLoad失败
   */
  public void bulkLoad(TextTrajParser parser, String inPath, String outPath, DataSetMeta dataSetMeta) throws Exception {
    instance = Database.getInstance();
    instance.createDataSet(dataSetMeta);
    logger.info("Starting bulk load dataset {}", dataSetMeta.getDataSetName());
    long start = System.currentTimeMillis();
    mainIndexBulkLoad(parser, inPath, outPath, dataSetMeta);
    secondaryIndexBulkLoad(outPath, dataSetMeta);
    logger.info("All indexes of Dataset [{}] have been loaded into HBase, total time cost: {}ms.",
        dataSetMeta.getDataSetName(),
        System.currentTimeMillis() - start);
  }
}
