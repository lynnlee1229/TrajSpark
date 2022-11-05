package cn.edu.whu.trajspark.database.load.driver;

import static cn.edu.whu.trajspark.constant.DBConstants.DATA_TABLE_SUFFIX;

import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.mapper.TextMapper;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TrajectoryDataDriver extends Configured {

  public  static final String PROCESS_INPUT_CONF_KEY = "import.process.input.path";
  public  static final String FILE_OUTPUT_CONF_KEY = "import.file.output.path";
  public  static final String TABLE_NAME_CONF_KEY = "table.name";

  static Database instance;

  private void setUpConfiguration(String inPath, String outPath, String tableName) {
    Configuration conf = getConf();
    conf.set(PROCESS_INPUT_CONF_KEY, inPath);
    conf.set(FILE_OUTPUT_CONF_KEY, outPath);
    conf.set(TABLE_NAME_CONF_KEY, tableName);
  }

  /**
   *  Configure MapReduce for bulkLoad
   * @param conf hbase conf
   * @param dataTable Target table for storing data
   * @return running state
   * @throws IOException ..
   */

  private int doBulkLoad(Configuration conf, DataTable dataTable) throws IOException {
    Path inPath = new Path(conf.get(PROCESS_INPUT_CONF_KEY));
    Path outPath = new Path(conf.get(FILE_OUTPUT_CONF_KEY));
    String tableName = conf.get(TABLE_NAME_CONF_KEY);
    Job job = Job.getInstance(conf, "Batch Import HBase Table：" + tableName);
    job.setJarByClass(TrajectoryDataDriver.class);
    FileInputFormat.setInputPaths(job, inPath);
    FileSystem fs = outPath.getFileSystem(conf);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }
    FileOutputFormat.setOutputPath(job, outPath);
    TextMapper.setDataTable(dataTable);
    job.setMapperClass(TextMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setNumReduceTasks(0);
    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
    try (Admin admin = instance.getAdmin(); Table table = instance.getTable(tableName)) {
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

  /**
   * Initialize database instance
   * @param dataSetMeta index information
   * @throws IOException ..
   */
  public void initDataSetTest(DataSetMeta dataSetMeta) throws IOException {
    instance = Database.getInstance();
    instance.openConnection();
    instance.createDataSet(dataSetMeta);
  }

  public int bulkLoad(String inPath, String outPath, DataSetMeta dataSetMeta) throws Exception {
    initDataSetTest(dataSetMeta);
    String tableName = dataSetMeta.getDataSetName() + DATA_TABLE_SUFFIX;
    DataTable dataTable = instance.getDataTable(dataSetMeta.getDataSetName());
    setUpConfiguration(inPath, outPath, tableName);
    long startLoadTime = System.currentTimeMillis();
    int status = doBulkLoad(getConf(), dataTable);
    long endLoadTime = System.currentTimeMillis();
    long bulkLoadTime = endLoadTime - startLoadTime;
    System.out.println("Load data table time:" + bulkLoadTime / 1000 + "s");
    instance.closeConnection();
    return status;
  }
}
