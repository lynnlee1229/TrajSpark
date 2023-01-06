package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TrajectoryDataMapper {

  private static final Logger LOGGER = Logger.getLogger(TrajectoryDataMapper.class);
  private static byte[] mainIndexKey;

  public static byte[] getMapRowKey(Trajectory trajectory, IndexMeta indexMeta) {
    ByteArray index = indexMeta.getIndexStrategy().index(trajectory);
    return index.getBytes();
  }

  /**
   * Convert a trajectory to a put object based on index information
   *
   * @param trajectory trajectory
   * @param indexMeta  index information
   * @return hbase put
   * @throws IOException ..
   */

  public static Put getMapPut(Trajectory trajectory, IndexMeta indexMeta) throws IOException {
    Put put = null;
    if (indexMeta.isMainIndex()) {
      put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
      mainIndexKey = put.getRow();
    } else {
      put = TrajectorySerdeUtils.getPutForSecondaryIndex(indexMeta, trajectory, mainIndexKey);
    }
    return put;
  }

  public static void configureHFilesOnHDFS(Database instance, String tableName, Job job)
      throws IOException {
    Table table = instance.getTable(tableName);
    RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
  }

  public static List<Put> mapTrajectoryToRow(Trajectory trajectory, List<IndexMeta> indexMetaList)
      throws IOException {
    ArrayList<Put> putArrayList = new ArrayList<>();
    for (IndexMeta indexMeta : indexMetaList) {
      final byte[] rowKey = getMapRowKey(trajectory, indexMeta);
      Put put = getMapPut(trajectory, indexMeta);
      if (rowKey == null || rowKey.length <= 0) {
        LOGGER.info("Trajectory Key is invalid");
      } else {
        putArrayList.add(put);
      }
    }
    return putArrayList;
  }

}
