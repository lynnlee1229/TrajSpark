package cn.edu.whu.trajspark.core.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/11/1
 **/
public class IOUtils {

  public static List<String> getFileNames(String fatherPath) {
    List<String> files = new ArrayList<>();
    File fatherFile = new File(fatherPath);
    File[] fileList = fatherFile.listFiles();

    for (int i = 0; i < (fileList != null ? fileList.length : 0); ++i) {
      if (fileList[i].isFile()) {
        files.add(fileList[i].toString());
      }
    }
    return files;
  }


  /**
   * 按行读取CSV文件
   *
   * @param filePath 文件路径（包括文件名）
   * @param hasTitle 是否有标题行
   * @return
   */
  public static List<String> readCSV(String filePath, boolean hasTitle) {
    List<String> data = new ArrayList<>();
    String line = null;
    try {
      BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "utf-8"));
      if (hasTitle) {
        //第一行信息，为标题信息，不返回
        line = bufferedReader.readLine();
//        data.add(line);
        System.out.println("标题行：" + line);
      }
      while ((line = bufferedReader.readLine()) != null) {
        //数据行
        data.add(line);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return data;
  }

  /**
   * 一次性读取文本文件
   *
   * @param fileName 文件名
   * @return
   */
  public static String readFileToString(String fileName) {
    String enCoding = "UTF-8";
    File file = new File(fileName);
    Long fileLength = file.length();
    byte[] fileContent = new byte[fileLength.intValue()];
    try {
      FileInputStream in = new FileInputStream(file);
      in.read(fileContent);
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      return new String(fileContent, enCoding);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static String readFileToString(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    StringBuilder stringBuilder = new StringBuilder();
    String s = "";
    while ((s = br.readLine()) != null) {
      stringBuilder.append(s);
    }
    return stringBuilder.toString();
  }

  public static void writeStringToFile(String fileName, String content) throws IOException {
    try {
      File file = new File(fileName);
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }


}
