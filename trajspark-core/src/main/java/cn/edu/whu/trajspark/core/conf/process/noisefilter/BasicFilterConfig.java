package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class BasicFilterConfig implements IFilterConfig {
  private double maxSpeed;
  private double minTrajLength;

  @JsonCreator
  public BasicFilterConfig(@JsonProperty("maxSpeed") double maxSpeed,
                           @JsonProperty("minTrajLength") double minTrajLength) {
    this.maxSpeed = maxSpeed;
    this.minTrajLength = minTrajLength;
  }

  public double getMaxSpeed() {
    return maxSpeed;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  public void setMaxSpeed(double maxSpeed) {
    this.maxSpeed = maxSpeed;
  }

  public void setMinTrajLength(double minTrajLength) {
    this.minTrajLength = minTrajLength;
  }

  @Override
  public FilterEnum getFilterType() {
    return FilterEnum.BASIC_FILTER;
  }

}
