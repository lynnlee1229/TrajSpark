package cn.edu.whu.trajspark.core.conf.process.segmenter;

import cn.edu.whu.trajspark.core.conf.process.detector.IDectorConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class StayPointBasedSegmenterConfig implements ISegmenterConfig {

  private double minTrajLength;

  private IDectorConfig dectorConfig;

  @JsonCreator
  public StayPointBasedSegmenterConfig(
      @JsonProperty("minTrajLength") double minTrajLength,
      @JsonProperty("dectorConfig") IDectorConfig dectorConfig) {
    this.minTrajLength = minTrajLength;
    this.dectorConfig = dectorConfig;
  }

  public IDectorConfig getDectorConfig() {
    return dectorConfig;
  }

  public void setDectorConfig(IDectorConfig dectorConfig) {
    this.dectorConfig = dectorConfig;
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
