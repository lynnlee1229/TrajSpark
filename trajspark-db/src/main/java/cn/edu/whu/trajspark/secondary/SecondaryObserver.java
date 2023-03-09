package cn.edu.whu.trajspark.secondary;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.index.IndexType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * DB在为某数据集使用Put添加数据时，仅会在主数据表直接作PUT操作，对于非主数据表的主索引、辅助索引，
 * 均需借助本Observer完成相应的Put操作。
 * TODO: 创建数据集的主数据表时，以Dynamic Loading形式为主数据表加载该Observer。
 *
 * @author Haocheng Wang
 * Created on 2023/2/1
 */
public class SecondaryObserver implements RegionObserver {

  private static final Logger logger = LoggerFactory.getLogger(SecondaryObserver.class);

  /***
   * 在主索引表的put完成后，对其他主索引表、辅助索引表添加相应的记录。
   */
  @Override
  public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit) throws IOException {
    Database instance = Database.getInstance();
    RegionObserver.super.postPut(c, put, edit);
    // 根据主数据表的Put对象还原待插入轨迹
    byte[] mainIndexTableRowKey = put.getRow();
    Trajectory trajectory = getTrajectoryFromPut(put);
    // 获取待处理的主索引、辅助索引表名称
    String datasetName = IndexTable.extractDataSetName(c.getEnvironment().getRegionInfo().getTable().getNameAsString());
    DataSet dataSet = instance.getDataSet(datasetName);
    DataSetMeta dataSetMeta = dataSet.getDataSetMeta();
    IndexMeta mainIndexMeta = dataSetMeta.getCoreIndexMeta();
    Map<IndexType, List<IndexMeta>> map = dataSetMeta.getAvailableIndexes();
    // 2. 逐个构建Put对象，并执行Put
    for (IndexType indexType :map.keySet()) {
      for (IndexMeta indexMeta : map.get(indexType)) {
        if (!indexMeta.equals(mainIndexMeta)) {
          dataSet.getIndexTable(indexMeta).put(trajectory, mainIndexTableRowKey);
        }
      }
    }
  }

  private Trajectory getTrajectoryFromPut(Put put) throws IOException {
    return TrajectorySerdeUtils.getTrajectoryFromPut(put);
  }

}
