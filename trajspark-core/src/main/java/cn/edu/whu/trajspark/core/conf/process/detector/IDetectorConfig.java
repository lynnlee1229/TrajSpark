package cn.edu.whu.trajspark.core.conf.process.detector;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = BasicDetectorConfig.class, name = "BASIC_DETECTOR")
})
public interface IDetectorConfig extends Serializable {
  DetectorEnum getDetectorType();
}
