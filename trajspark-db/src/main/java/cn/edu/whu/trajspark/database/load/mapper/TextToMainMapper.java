package cn.edu.whu.trajspark.database.load.mapper;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.load.TextTrajParser;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

/**
 * 从Text文件中读取数据，并将其转换为Main Index的put对象。
 *
 * @author Xu Qi
 * @since 2022/11/1
 */
public class TextToMainMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  private static IndexTable indexTable;
  private static TextTrajParser parser;

  public static void config(IndexTable indexTable, TextTrajParser parser) {
    TextToMainMapper.indexTable = indexTable;
    TextToMainMapper.parser = parser;
  }

  private byte[] getMapRowKey(Trajectory trajectory, IndexMeta indexMeta) {
    ByteArray index = indexMeta.getIndexStrategy().index(trajectory);
    return index.getBytes();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String lineValue = value.toString();
    System.out.println(lineValue);
    Trajectory trajectory = null;
    try {
      trajectory = parser.parse(lineValue);
      IndexMeta indexMeta = indexTable.getIndexMeta();
      Put put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
      context.write(new ImmutableBytesWritable(put.getRow()), put);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
