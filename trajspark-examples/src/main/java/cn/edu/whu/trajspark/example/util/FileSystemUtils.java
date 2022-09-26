package cn.edu.whu.trajspark.example.util;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class FileSystemUtils {
  /**
   * logger
   */
  private static final Logger LOGGER = Logger.getLogger(FileSystemUtils.class);

  /**
   * 读取文件
   *
   * @param filePath 文件路径
   * @return 文件字符串
   */
  public static String readFully(String fsDefaultName, String filePath) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", fsDefaultName);
    Path path = new Path(filePath);
    try (FileSystem fs = FileSystem.get(URI.create(filePath), conf);
         FSDataInputStream in = fs.open(path)) {
      FileStatus stat = fs.getFileStatus(path);
      byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
      in.readFully(0, buffer);
      return new String(buffer);
    } catch (IOException e) {
      LOGGER.error(e.getMessage() + "/nFailed to read file from : " + filePath);
      e.printStackTrace();
      return null;
    }
  }
}
