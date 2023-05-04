package cn.edu.whu.trajspark.core.util;

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
 * @date 2023/4/24
 **/
public class FSUtils {
  @SuppressWarnings("checkstyle:ConstantName")
  private static final Logger logger = Logger.getLogger(FSUtils.class);

  /**
   * read file from hdfs
   * @param fsDefaultName
   * @param filePath
   * @return
   */
  public static String readFromFS(String fsDefaultName, String filePath) {
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
      logger.error(e.getMessage() + "/nFailed to read file from : " + filePath);
      e.printStackTrace();
      return null;
    }
  }
}
