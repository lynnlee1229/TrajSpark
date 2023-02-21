package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.load.BulkLoadUtils;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/20
 */
public class TableBulkLoadDriver extends Configured {

  /**
   * 本方法以核心索引表中的轨迹为数据源，按照新增IndexMeta转换，并BulkLoad至HBase中。
   * 需要配合DataSet类中使用，DataSet类中负责将待添加index meta的信息更新至dataSetMeta中，但不负责数据的实际写入。
   */
  public void bulkLoad(String tempOutPutPath, IndexMeta newIndex) throws IOException {
    Configuration conf = getConf();
    conf.set(DBConstants.BULK_LOAD_TEMP_FILE_PATH, tempOutPutPath);
    BulkLoadUtils.createIndexFromTable(conf, newIndex);
  }
}
