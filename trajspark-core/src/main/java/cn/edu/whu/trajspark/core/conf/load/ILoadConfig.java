package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/15
 **/
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({
    @JsonSubTypes.Type(
        value = HDFSLoadConfig.class,
        name = "hdfs"
    ),
    @JsonSubTypes.Type(
        value = StandaloneLoadConfig.class,
        name = "standalone"
    )
})

public interface ILoadConfig extends Serializable {
  InputType getInputType();

  String getFsDefaultName();

  enum InputType implements Serializable {
    STANDALONE("standalone"),
    HDFS("hdfs"),
    HBASE("hbase"),
    GEOMESA("geomesa");

    private String inputType;

    InputType(String inputType) {
      this.inputType = inputType;
    }

    public final String toString() {
      return "InputType{type='" + this.inputType + '\'' + '}';
    }
  }
}
