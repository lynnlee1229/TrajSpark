package cn.edu.whu.trajspark.core.conf.process.detector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class BasicDetectorConfig implements IDectorConfig {
  private double maxStayDistInMeter;
  private double maxStayTimeInSecond;
  @JsonCreator
  public BasicDetectorConfig(@JsonProperty("maxStayDistInMeter") double maxStayDistInMeter,
                             @JsonProperty("maxStayTimeInSecond") double maxStayTimeInSecond) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
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

  @Override
  public DetectorEnum getDetectorType() {
    return DetectorEnum.BASIC_DETECTOR;
  }
}
