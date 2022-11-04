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

  @JsonCreator
  public StandaloneStoreConfig(
      @JsonProperty("location") String location,
      @JsonProperty("schema") StoreSchemaEnum schema) {
    this.location = location;
    this.schema = schema;
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
