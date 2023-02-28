package cn.edu.whu.trajspark.database.load;

import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.driver.TextBulkLoadDriver;
import cn.edu.whu.trajspark.database.load.mapper.MainToMainMapper;
import cn.edu.whu.trajspark.database.load.mapper.MainToSecondaryMapper;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/20
 */
public class BulkLoadUtils {

  private static Database instance;

  static {
    try {
      instance = Database.getInstance();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 本方法以核心索引表中的轨迹为数据源，按照新增IndexMeta转换，并BulkLoad至HBase中。
   * 执行此方法时，应确保DataSetMeta中已有本Secondary Table的信息。
   */
  public static void createIndexFromTable(Configuration conf, IndexMeta indexMeta, DataSetMeta dataSetMeta) throws IOException {
    Path outPath = new Path(conf.get(DBConstants.BULK_LOAD_TEMP_FILE_PATH));
    String inputTableName = dataSetMeta.getCoreIndexMeta().getIndexTableName();
    String outTableName = indexMeta.getIndexTableName();
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + outTableName);
    job.setJarByClass(TextBulkLoadDriver.class);
    // 设置MapReduce任务输出的路径
    FileSystem fs = outPath.getFileSystem(conf);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }
    FileOutputFormat.setOutputPath(job, outPath);

    // 配置Map算子，根据待写入Index是否为主索引，选择对应的Mapper实现。
    if (indexMeta.isMainIndex()) {
      MainToMainMapper.setMainTable(new IndexTable(indexMeta));
      TableMapReduceUtil.initTableMapperJob(inputTableName,
          buildCoreIndexScan(),
          MainToMainMapper.class,
          ImmutableBytesWritable.class,
          Put.class,
          job);
    } else {
      MainToSecondaryMapper.setSecondaryTable(new IndexTable(indexMeta));
      TableMapReduceUtil.initTableMapperJob(inputTableName,
          buildCoreIndexScan(),
          MainToSecondaryMapper.class,
          ImmutableBytesWritable.class,
          Put.class,
          job);
    }

    // 配置Reduce算子
    job.setNumReduceTasks(1);
    job.setReducerClass(PutSortReducer.class);

    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(outTableName));
    try (Admin admin = instance.getAdmin(); Table table = instance.getTable(outTableName)) {
      HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
      if (!job.waitForCompletion(true)) {
        return;
      }
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
      loader.doBulkLoad(outPath, admin, table, locator);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Scan buildCoreIndexScan() {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(DBConstants.INDEX_TABLE_CF));
    return scan;
  }
}
