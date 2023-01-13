package cn.edu.whu.trajspark.database.load.mapper.datatypes;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author Xu Qi
 * @since 2023/1/12
 */
public class KeyValueInfo {

  private String qualifier;
  private KeyValue value;

  public KeyValueInfo(String qualifier, KeyValue value) {
    this.qualifier = qualifier;
    this.value = value;
  }

  public String getQualifier() {
    return qualifier;
  }

  public KeyValue getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "KeyValueInfo{" +
        "qualifier='" + qualifier + '\'' +
        '}';
  }
}
