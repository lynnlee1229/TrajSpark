package cn.edu.whu.trajspark.core.conf.process.segmenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class StayPointBasedSegmenterConfig implements ISegmenterConfig {
  private double maxStayDistInMeter;

  private double maxStayTimeInSecond;
  private double minTrajLength;

  @JsonCreator
  public StayPointBasedSegmenterConfig(
      @JsonProperty("maxStayDistInMeter") double maxStayDistInMeter,
      @JsonProperty("maxStayTimeInSecond") double maxStayTimeInSecond,
      @JsonProperty("minTrajLength") double minTrajLength) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
    this.minTrajLength = minTrajLength;
  }

  public double getMaxStayDistInMeter() {
    return maxStayDistInMeter;
  }

  public void setMaxStayDistInMeter(double maxStayDistInMeter) {
    this.maxStayDistInMeter = maxStayDistInMeter;
  }

  public double getMaxStayTimeInSecond() {
    return maxStayTimeInSecond;
  }

  public void setMaxStayTimeInSecond(double maxStayTimeInSecond) {
    this.maxStayTimeInSecond = maxStayTimeInSecond;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  public void setMinTrajLength(double minTrajLength) {
    this.minTrajLength = minTrajLength;
  }

  @Override
  public SegmenterEnum getSegmenterType() {
    return SegmenterEnum.STAYPOINTBASED_SEGMENTER;
  }
}
