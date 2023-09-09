package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.database.load.driver.JsonWriteFormat;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/11/4
 */
class JsonWriteFormatTest extends TestCase {

  @Test
  public void testWrite() throws IOException {
    String inPath = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String outPath =
        Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).getPath() + "traj_json/formatTra.txt";
    JsonWriteFormat.writeJson(inPath, outPath);
  }
}