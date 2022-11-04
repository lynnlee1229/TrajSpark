package cn.edu.whu.trajspark.core.conf.store;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/11/4
 **/
public enum StoreType implements Serializable {
  STANDALONE("standalone"),
  HDFS("hdfs"),
  HBASE("hbase"),
  GEOMESA("geoMesa");

  private String type;

  StoreType(String type) {
    this.type = type;
  }

  public String toString() {
    return "OutputType{type='" + this.type + '\'' + '}';
  }

  static class Constants {
    static final String HDFS = "hdfs";
    static final String STANDALONE = "standalone";
    static final String HBASE = "hbase";
    static final String GEOMESA = "geoMesa";
  }
}
