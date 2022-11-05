package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TrajectoryDataMapper extends Mapper<LongWritable, List<Trajectory>, ImmutableBytesWritable, Put> {

  // TODO: 2022/11/4 Rdd bulkload datamap
  @Override
  protected void map(LongWritable key, List<Trajectory> value,
      Mapper<LongWritable, List<Trajectory>, ImmutableBytesWritable, Put>.Context context)
      throws IOException, InterruptedException {
    super.map(key, value, context);
  }
}
