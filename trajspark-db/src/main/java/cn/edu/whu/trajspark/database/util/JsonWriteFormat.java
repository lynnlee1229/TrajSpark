package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * @author Xu Qi
 * @since 2022/11/4
 */
public class JsonWriteFormat {

  /**
   * json write to line
   * @param inPath json path
   * @param outPath output path
   * @throws IOException ..
   */
  public static void writeJson(String inPath, String outPath) throws IOException {
    String text = JSONUtil.readLocalTextFile(inPath);
    JSONObject feature = JSONObject.parseObject(text);
    JSONArray jsonObject = feature.getJSONArray("features");
    FileOutputStream fileOutputStream = new FileOutputStream(new File(outPath));
    OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
    BufferedWriter bw = new BufferedWriter(outputStreamWriter);
    for (int i = 0; i < jsonObject.size(); i++) {
      JSONObject object = jsonObject.getJSONObject(i);
      bw.write(object.toString() + "\n");
    }
    bw.close();
    outputStreamWriter.close();
    fileOutputStream.close();
  }
}
