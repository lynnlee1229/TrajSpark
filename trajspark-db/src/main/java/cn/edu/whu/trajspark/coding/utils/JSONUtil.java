package cn.edu.whu.trajspark.coding.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
public class JSONUtil {

  /**
   * read full text file
   * @param path text path
   * @return string
   */
  public static String readLocalTextFile(String path) {
    File file = new File(path);
    StringBuilder sb = new StringBuilder();
    try {
      Reader reader = new InputStreamReader(Files.newInputStream(file.toPath()));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return sb.toString();
  }
}
