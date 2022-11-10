package cn.edu.whu.trajspark.core.conf.store;

import cn.edu.whu.trajspark.core.enums.StoreSchemaEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/9/22
 **/
public class StandaloneStoreConfig implements IStoreConfig {

  private String location;
  private StoreSchemaEnum schema;

  private String splitter;
  private String lineBreaker;

  private String filePostFix;

  @JsonCreator
  public StandaloneStoreConfig(
      @JsonProperty("location") String location,
      @JsonProperty("schema") StoreSchemaEnum schema,
      @JsonProperty("splitter") String splitter,
      @JsonProperty("lineBreaker") String lineBreaker,
      @JsonProperty("filePostFix") String filePostFix) {
    this.location = location;
    this.schema = schema;
    this.splitter = splitter;
    this.lineBreaker = lineBreaker;
    this.filePostFix = filePostFix;
  }

  public String getFilePostFix() {
    return filePostFix;
  }

  public String getSplitter() {
    return splitter;
  }

  public String getLineBreaker() {
    return lineBreaker;
  }

  public StoreType getStoreType() {
    return StoreType.STANDALONE;
  }

  public String getLocation() {
    return this.location;
  }

  public StoreSchemaEnum getSchema() {
    return this.schema;
  }
}
