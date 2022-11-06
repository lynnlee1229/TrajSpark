package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = BasicFilterConfig.class, name = "BASIC_FILTER"),
    @JsonSubTypes.Type(value = PingpongFilterConfig.class, name = "PINGPONG_FILTER"),
    @JsonSubTypes.Type(value = DriftFilterConfig.class, name = "DRIFT_FILTER"),
    @JsonSubTypes.Type(value = CompositiveFilterConfig.class, name = "COMPOSITIVEFILTER_FILTER")
})
public interface IFilterConfig extends Serializable {
  FilterEnum getFilterType();
}

