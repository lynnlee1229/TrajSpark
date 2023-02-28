package cn.edu.whu.trajspark.database.load.driver;

import cn.edu.whu.trajspark.constant.DBConstants;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.load.BulkLoadDriverUtils;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
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
   * 需要配合{@link Database#addIndexMeta}使用，该方法负责将待
   * 添加index meta的信息更新至dataSetMeta中，并创建新表，但不负责数据的实际写入。
   * 数据写入由本类负责。
   */
  public void bulkLoad(String tempOutPutPath, IndexMeta newIndex, DataSetMeta dataSetMeta) throws IOException {
    Configuration conf = getConf();
    conf.set(DBConstants.BULK_LOAD_TEMP_FILE_PATH_KEY, tempOutPutPath);
    BulkLoadDriverUtils.createIndexFromTable(conf, newIndex, dataSetMeta);
  }
}
