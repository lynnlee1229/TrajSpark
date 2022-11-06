package cn.edu.whu.trajspark.core.conf.process.segmenter;

import cn.edu.whu.trajspark.core.conf.process.detector.IDetectorConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class StayPointBasedSegmenterConfig implements ISegmenterConfig {

  private double minTrajLength;

  private IDetectorConfig detectorConfig;

  @JsonCreator
  public StayPointBasedSegmenterConfig(
      @JsonProperty("minTrajLength") double minTrajLength,
      @JsonProperty("detectorConfig") IDetectorConfig detectorConfig) {
    this.minTrajLength = minTrajLength;
    this.detectorConfig = detectorConfig;
  }

  public IDetectorConfig getDetectorConfig() {
    return detectorConfig;
  }

  public void setDetectorConfig(
      IDetectorConfig detectorConfig) {
    this.detectorConfig = detectorConfig;
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
