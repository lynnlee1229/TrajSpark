package cn.edu.whu.trajspark.core.conf.process.segmenter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = BasicSegmenterConfig.class, name = "BASIC_SEGMENTER"),
    @JsonSubTypes.Type(value = StayPointBasedSegmenterConfig.class, name = "STAYPOINTBASED_SEGMENTER"),
    @JsonSubTypes.Type(value = UserDefinedSegmenterConfig.class, name = "USERDEFINED_SEGMENTER")
})
public interface ISegmenterConfig extends Serializable {
  SegmenterEnum getSegmenterType();
}
