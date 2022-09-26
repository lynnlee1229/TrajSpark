package cn.edu.whu.trajspark.core.conf.store;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({@JsonSubTypes.Type(
    value = HDFSStoreConfig.class,
    name = "hdfs"
)})
//, @JsonSubTypes.Type(
//    value = FileOutputConfig.class,
//    name = "file"
//), @JsonSubTypes.Type(
//    value = GeoMesaOutputConfig.class,
//    name = "geoMesa"
//)
public interface IStoreConfig extends Serializable {
  StoreType getStoreType();

  enum StoreType implements Serializable {
    FILE("file"),
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
      static final String FILE = "file";
      static final String HBASE = "hbase";
      static final String GEOMESA = "geoMesa";
    }
  }
}
