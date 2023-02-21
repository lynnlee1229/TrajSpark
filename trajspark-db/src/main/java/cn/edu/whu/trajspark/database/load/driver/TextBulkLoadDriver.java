package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.BulkLoadUtils;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import cn.edu.whu.trajspark.database.load.mapper.TextToMainMapper;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.BULK_LOAD_TEMP_FILE_PATH;
import static cn.edu.whu.trajspark.constant.DBConstants.PROCESS_INPUT_CONF_KEY;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TextBulkLoadDriver extends Configured {

  private static Logger logger = LoggerFactory.getLogger(TextBulkLoadDriver.class);

  static Database instance;

  private void setUpConfiguration(String inPath, String outPath) {
    Configuration conf = getConf();
    conf.set(PROCESS_INPUT_CONF_KEY, inPath);
    conf.set(BULK_LOAD_TEMP_FILE_PATH, outPath);
  }

  /**
   * 将文本文件中的数据bulk load到某索引表中
   */
  private void runTextToMainBulkLoad(Class<? extends Mapper> cls, Configuration conf, IndexTable indexTable) throws IOException {
    Path inPath = new Path(conf.get(PROCESS_INPUT_CONF_KEY));
    Path outPath = new Path(conf.get(DBConstants.BULK_LOAD_TEMP_FILE_PATH));
    String tableName = indexTable.getIndexMeta().getIndexTableName();
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + tableName);
    job.setJarByClass(TextBulkLoadDriver.class);
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

  private void textToMainIndexes(TextTrajParser parser, DataSetMeta dataSetMeta) throws Exception {
    // 多个dataset meta，先处理其中的主索引，后处理其中的辅助索引
    List<IndexMeta> indexMetaList = dataSetMeta.getIndexMetaList();
    for (IndexMeta im : indexMetaList) {
      if (im.isMainIndex()) {
        long startLoadTime = System.currentTimeMillis();
        logger.info("Starting bulk load main index, meta: {}", im);
        IndexTable indexTable = new IndexTable(im);
        try {
          TextToMainMapper.config(indexTable, parser::parse);
          runTextToMainBulkLoad(TextToMainMapper.class, getConf(), indexTable);
        } catch (Exception e) {
          logger.error("Failed to finish bulk load main index {}", im, e);
          throw e;
        }
        long endLoadTime = System.currentTimeMillis();
        logger.info("Main index {} load finished, cost time: {}ms.", im.getIndexTableName(), (endLoadTime - startLoadTime));
      }
    }
  }

  private void tableToSecondaryIndexes(DataSetMeta dataSetMeta) throws Exception {
    for (IndexMeta im : dataSetMeta.getIndexMetaList()) {
      if (!im.isMainIndex()) {
        BulkLoadUtils.createIndexFromTable(getConf(), im);
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
    logger.info("Starting bulk load dataset {}", dataSetMeta.getDataSetName());
    long start = System.currentTimeMillis();
    setUpConfiguration(inPath, outPath);
    // 利用文本文件将数据导入至所有主数据表中
    textToMainIndexes(parser, dataSetMeta);
    // 利用核心主索引表的数据，导入至所有辅助索引表中
    tableToSecondaryIndexes(dataSetMeta);
    logger.info("All indexes of Dataset [{}] have been loaded into HBase, total time cost: {}ms.",
        dataSetMeta.getDataSetName(),
        System.currentTimeMillis() - start);
  }
}
