package cn.edu.whu.trajspark.core.conf.process.segmenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class BasicSegmenterConfig implements ISegmenterConfig {
  private double maxDis;
  private double maxTimeInterval;
  private double minTrajLength;

  @JsonCreator
  public BasicSegmenterConfig(@JsonProperty("maxDis") double maxDis,
                              @JsonProperty("maxTimeInterval") double maxTimeInterval,
                              @JsonProperty("minTrajLength") double minTrajLength) {
    this.maxDis = maxDis;
    this.maxTimeInterval = maxTimeInterval;
    this.minTrajLength = minTrajLength;
  }

  public double getMaxDis() {
    return maxDis;
  }

  public void setMaxDis(double maxDis) {
    this.maxDis = maxDis;
  }

  public double getMaxTimeInterval() {
    return maxTimeInterval;
  }

  public void setMaxTimeInterval(double maxTimeInterval) {
    this.maxTimeInterval = maxTimeInterval;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  public void setMinTrajLength(double minTrajLength) {
    this.minTrajLength = minTrajLength;
  }

  @Override
  public SegmenterEnum getSegmenterType() {
    return SegmenterEnum.BASIC_SEGMENTER;
  }
}
