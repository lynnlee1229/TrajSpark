package cn.edu.whu.trajspark.database.load.mapper;

import static cn.edu.whu.trajspark.database.util.ParseJsonToTrajectory.parseJsonToTrajectory;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TextMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  private static DataTable dataTable;
  private byte[] mainIndexKey;

  public static void setDataTable(DataTable dataTable) {
    TextMapper.dataTable = dataTable;
  }

  private byte[] getMapRowKey(Trajectory trajectory, IndexMeta indexMeta) {
    ByteArray index = indexMeta.getIndexStrategy().index(trajectory);
    return index.getBytes();
  }

  /**
   * Convert a trajectory to a put object based on index information
   * @param trajectory trajectory
   * @param indexMeta  index information
   * @return hbase put
   * @throws IOException ..
   */

  private Put getMapPut(Trajectory trajectory, IndexMeta indexMeta) throws IOException {
    Put put = null;
    if (indexMeta.isMainIndex()) {
      put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
      mainIndexKey = put.getRow();
    } else {
      put = TrajectorySerdeUtils.getPutForSecondaryIndex(indexMeta, trajectory, mainIndexKey);
    }
    return put;
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String lineValue = value.toString();
    System.out.println(lineValue);
    Trajectory trajectory = parseJsonToTrajectory(lineValue);
    DataSetMeta dataSetMeta = dataTable.getDataSetMeta();
    List<IndexMeta> indexMetaList = dataSetMeta.getIndexMetaList();
    dataSetMeta.sortByMainIndexMeta(indexMetaList);
    for (IndexMeta indexMeta : indexMetaList) {
      final byte[] rowKey = getMapRowKey(trajectory, indexMeta);
      Put put = getMapPut(trajectory, indexMeta);
      if (rowKey == null || rowKey.length <= 0) {
        System.out.printf("Skipping record %d", key.get());
        context.getCounter("ImportText", "import.bad.line").increment(1);
      } else {
        context.write(new ImmutableBytesWritable(rowKey), put);
      }
    }
  }
}
