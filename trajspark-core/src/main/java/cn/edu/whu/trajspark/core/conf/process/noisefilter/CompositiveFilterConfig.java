package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/29
 **/
public class CompositiveFilterConfig implements IFilterConfig {
  private BasicFilterConfig basicFilterConfig;
  private PingpongFilterConfig pingpongFilterConfig;

  @JsonCreator
  public CompositiveFilterConfig(@JsonProperty("basicFilterConfig")BasicFilterConfig basicFilterConfig,
                                 @JsonProperty("pingpongFilterConfig")PingpongFilterConfig pingpongFilterConfig) {
    this.basicFilterConfig = basicFilterConfig;
    this.pingpongFilterConfig = pingpongFilterConfig;
  }
  public BasicFilterConfig getBasicFilterConfig() {
    return basicFilterConfig;
  }

  public void setBasicFilterConfig(
      BasicFilterConfig basicFilterConfig) {
    this.basicFilterConfig = basicFilterConfig;
  }

  public PingpongFilterConfig getPingpongFilterConfig() {
    return pingpongFilterConfig;
  }

  public void setPingpongFilterConfig(
      PingpongFilterConfig pingpongFilterConfig) {
    this.pingpongFilterConfig = pingpongFilterConfig;
  }
  @Override
  public FilterEnum getFilterType() {
    return FilterEnum.COMPOSITIVEFILTER_FILTER;
  }
}
