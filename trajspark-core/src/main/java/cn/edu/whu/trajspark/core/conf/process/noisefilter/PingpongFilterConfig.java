package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class PingpongFilterConfig implements IFilterConfig {
  private String baseStationIndex;
  private double maxPingpongTime;
  private double minTrajLength;

  @JsonCreator
  public PingpongFilterConfig(@JsonProperty("baseStationIndex") String baseStationIndex,
                              @JsonProperty("maxPingpongTime") double maxPingpongTime,
                              @JsonProperty("minTrajLength") double minTrajLength) {
    this.baseStationIndex = baseStationIndex;
    this.maxPingpongTime = maxPingpongTime;
    this.minTrajLength = minTrajLength;
  }

  public String getBaseStationIndex() {
    return baseStationIndex;
  }

  public void setBaseStationIndex(String baseStationIndex) {
    this.baseStationIndex = baseStationIndex;
  }

  public double getMaxPingpongTime() {
    return maxPingpongTime;
  }

  public void setMaxPingpongTime(double maxPingpongTime) {
    this.maxPingpongTime = maxPingpongTime;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  public void setMinTrajLength(double minTrajLength) {
    this.minTrajLength = minTrajLength;
  }

  @Override
  public FilterEnum getFilterType() {
    return FilterEnum.PINGPONG_FILTER;
  }
}
