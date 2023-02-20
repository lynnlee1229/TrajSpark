package cn.edu.whu.trajspark.core.conf.process.simplifier;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2023/2/16
 **/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DPSimplifierConfig.class, name = "DP_SIMPLIFIER")
})
public interface ISimplifierConfig extends Serializable {
  SimplifierEnum getSimplifierType();
}
