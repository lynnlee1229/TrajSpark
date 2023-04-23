package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static cn.edu.whu.trajspark.database.load.BulkLoadDriverUtils.getIndexTable;

/**
 * 从Core index表中读取数据，并将其转换为辅助索引表的put对象。
 *
 * @author Haocheng Wang
 * Created on 2023/2/17
 */
public class MainToSecondaryMapper extends TableMapper<ImmutableBytesWritable, Put> {

  private static IndexTable secondaryTable;

  @Override
  protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
    super.setup(context);
    secondaryTable = getIndexTable(context.getConfiguration());
  }

  @SuppressWarnings("rawtypes")
  public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job)
      throws IOException {
    TableMapReduceUtil.initTableMapperJob(table, scan, mapper, ImmutableBytesWritable.class, Result.class, job);
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result coreIndexRow, Context context) throws IOException, InterruptedException {
    byte[] coreIndexRowKey = key.get();
    Trajectory t = TrajectorySerdeUtils.getTrajectoryFromResult(coreIndexRow);
    Put p = TrajectorySerdeUtils.getPutForSecondaryIndex(secondaryTable.getIndexMeta(), t, coreIndexRowKey);
    context.write(new ImmutableBytesWritable(p.getRow()), p);
  }
}
