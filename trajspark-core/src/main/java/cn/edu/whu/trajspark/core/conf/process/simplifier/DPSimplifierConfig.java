package cn.edu.whu.trajspark.core.conf.process.simplifier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
public class DPSimplifierConfig implements ISimplifierConfig{
  double epsilon;
  double minTrajLength;
  @JsonCreator
  public DPSimplifierConfig(@JsonProperty("epsilon") double epsilon,
                            @JsonProperty("minTrajLength") double minTrajLength) {
    this.epsilon = epsilon;
    this.minTrajLength = minTrajLength;
  }

  public double getEpsilon() {
    return epsilon;
  }

  public double getMinTrajLength() {
    return minTrajLength;
  }

  @Override
  public SimplifierEnum getSimplifierType() {
    return SimplifierEnum.DP_SIMPLIFIER;
  }
}
