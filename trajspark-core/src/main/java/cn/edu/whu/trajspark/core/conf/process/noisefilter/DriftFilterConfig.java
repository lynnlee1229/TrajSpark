package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/11/6
 **/
public class DriftFilterConfig implements IFilterConfig {
  private double maxSpeed;
  private double minAlpha;
  private double maxRatio;
  private double minTrajLength;

  @JsonCreator
  public DriftFilterConfig(@JsonProperty("maxSpeed") double maxSpeed,
                           @JsonProperty("minAlpha") double minAlpha,
                           @JsonProperty("maxRatio") double maxRatio,
                           @JsonProperty("minTrajLength") double minTrajLength) {
    this.maxSpeed = maxSpeed;
    this.minAlpha = minAlpha;
    this.maxRatio = maxRatio;
    this.minTrajLength = minTrajLength;
  }

  public double getMaxSpeed() {
    return maxSpeed;
  }

  public void setMaxSpeed(double maxSpeed) {
    this.maxSpeed = maxSpeed;
  }

  public double getMinAlpha() {
    return minAlpha;
  }

  public void setMinAlpha(double minAlpha) {
    this.minAlpha = minAlpha;
  }

  public double getMaxRatio() {
    return maxRatio;
  }

  public void setMaxRatio(double maxRatio) {
    this.maxRatio = maxRatio;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  public void setMinTrajLength(double minTrajLength) {
    this.minTrajLength = minTrajLength;
  }

  @Override
  public FilterEnum getFilterType() {
    return FilterEnum.DRIFT_FILTER;
  }
}
