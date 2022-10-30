package cn.edu.whu.trajspark.core.conf.process.detector;

import cn.edu.whu.trajspark.core.common.constant.PreProcessDefaultConstant;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class BasicDetectorConfig implements IDectorConfig {
  private double maxStayDistInMeter;
  private double maxStayTimeInSecond;

  private String stayPointTagName = PreProcessDefaultConstant.DEFAULT_STAYPOINT_TAG;

  @JsonCreator
  public BasicDetectorConfig(@JsonProperty("maxStayDistInMeter") double maxStayDistInMeter,
                             @JsonProperty("maxStayTimeInSecond") double maxStayTimeInSecond,
                             @JsonProperty("stayPointTagName") String stayPointTagName) {
    this.maxStayDistInMeter = maxStayDistInMeter;
    this.maxStayTimeInSecond = maxStayTimeInSecond;
    this.stayPointTagName = stayPointTagName;
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

  public String getStayPointTagName() {
    return stayPointTagName;
  }

  public void setStayPointTagName(String stayPointTagName) {
    this.stayPointTagName = stayPointTagName;
  }

  @Override
  public DetectorEnum getDetectorType() {
    return DetectorEnum.BASIC_DETECTOR;
  }
}
