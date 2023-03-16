package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

import static cn.edu.whu.trajspark.database.load.BulkLoadDriverUtils.getIndexTable;
import static cn.edu.whu.trajspark.database.load.BulkLoadDriverUtils.getTextParser;

/**
 * 从Text文件中读取数据，并将其转换为Main Index的put对象。
 *
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TextToMainMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  private static IndexTable indexTable;
  private static TextTrajParser parser;


  @Override
  protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
    super.setup(context);
    try {
      indexTable = getIndexTable(context.getConfiguration());
      parser = getTextParser(context.getConfiguration());
    } catch (InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String lineValue = value.toString();
    Trajectory trajectory;
    try {
      trajectory = parser.parse(lineValue);
      IndexMeta indexMeta = indexTable.getIndexMeta();
      Put put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
      context.write(new ImmutableBytesWritable(put.getRow()), put);
    } catch (ParseException e) {
      e.printStackTrace();
      System.out.println("Write failed:" + value);
    }
  }
}
