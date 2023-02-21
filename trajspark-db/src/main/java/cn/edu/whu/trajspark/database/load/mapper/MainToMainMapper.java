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

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/20
 */
public class MainToMainMapper extends TableMapper<ImmutableBytesWritable, Put> {

  private static IndexTable output;

  public static void setMainTable(IndexTable indexTable) {
    MainToMainMapper.output = indexTable;
  }

  @SuppressWarnings("rawtypes")
  public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job)
      throws IOException {
    TableMapReduceUtil.initTableMapperJob(table, scan, mapper, ImmutableBytesWritable.class, Result.class, job);
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result coreIndexRow, Context context) throws IOException, InterruptedException {
    Trajectory t = TrajectorySerdeUtils.getTrajectoryFromResult(coreIndexRow);
    Put p = TrajectorySerdeUtils.getPutForMainIndex(output.getIndexMeta(), t);
    context.write(new ImmutableBytesWritable(p.getRow()), p);
  }
}