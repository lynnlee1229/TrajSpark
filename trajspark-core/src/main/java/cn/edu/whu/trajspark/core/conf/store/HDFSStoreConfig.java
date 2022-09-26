package cn.edu.whu.trajspark.core.conf.store;

import cn.edu.whu.trajspark.core.enums.StoreSchemaEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/9/22
 **/
public class HDFSStoreConfig implements IStoreConfig {
  private String ip;
  private int port;
  private String location;
  private StoreSchemaEnum schema;

  @JsonCreator
  public HDFSStoreConfig(@JsonProperty("ip") String ip, @JsonProperty("port") int port, @JsonProperty("location") String location, @JsonProperty("schema") StoreSchemaEnum schema) {
    this.ip = ip;
    this.port = port;
    this.location = location;
    this.schema = schema;
  }

  public IStoreConfig.StoreType getStoreType() {
    return StoreType.HDFS;
  }

  public String getIp() {
    return this.ip;
  }

  public int getPort() {
    return this.port;
  }

  public String getLocation() {
    return this.location;
  }

  public StoreSchemaEnum getSchema() {
    return this.schema;
  }
}
