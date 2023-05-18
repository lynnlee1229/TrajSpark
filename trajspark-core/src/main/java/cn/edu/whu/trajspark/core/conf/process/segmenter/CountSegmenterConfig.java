package cn.edu.whu.trajspark.core.conf.process.segmenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2023/5/3
 **/
public class CountSegmenterConfig implements ISegmenterConfig {
  private final int minSegNum;
  private final int maxSegNum;
  private final double maxTimeInterval;
  private final Long seed;

  @JsonCreator
  public CountSegmenterConfig(@JsonProperty("minSegNum") int minSegNum,
                              @JsonProperty("maxSegNum") int maxSegNum,
                              @JsonProperty("maxTimeInterval") double maxTimeInterval,
                              @JsonProperty("seed") long seed) {
    this.minSegNum = minSegNum;
    this.maxSegNum = maxSegNum;
    this.maxTimeInterval = maxTimeInterval;
    this.seed = seed;
  }

  public int getMinSegNum() {
    return minSegNum;
  }

  public int getMaxSegNum() {
    return maxSegNum;
  }

  public double getMaxTimeInterval() {
    return maxTimeInterval;
  }

  public Long getSeed() {
    return seed;
  }

  @Override
  public SegmenterEnum getSegmenterType() {
    return SegmenterEnum.COUNT_SEGMENTER;
  }
}
