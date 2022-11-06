package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/29
 **/
public class CompositiveFilterConfig implements IFilterConfig {
  private PingpongFilterConfig pingpongFilterConfig;
  private DriftFilterConfig driftFilterConfig;

  @JsonCreator
  public CompositiveFilterConfig(@JsonProperty("driftFilterConfig")DriftFilterConfig driftFilterConfig,
                                 @JsonProperty("pingpongFilterConfig")PingpongFilterConfig pingpongFilterConfig) {
    this.driftFilterConfig = driftFilterConfig;
    this.pingpongFilterConfig = pingpongFilterConfig;
  }

  public DriftFilterConfig getDriftFilterConfig() {
    return driftFilterConfig;
  }

  public void setDriftFilterConfig(
      DriftFilterConfig driftFilterConfig) {
    this.driftFilterConfig = driftFilterConfig;
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
