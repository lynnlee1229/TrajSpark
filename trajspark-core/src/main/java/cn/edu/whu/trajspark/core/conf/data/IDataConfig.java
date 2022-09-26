package cn.edu.whu.trajspark.core.conf.data;

import cn.edu.whu.trajspark.core.enums.DataTypeEnum;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({@JsonSubTypes.Type(
    value = TrajectoryConfig.class,
    name = "trajectory"
), @JsonSubTypes.Type(
    value = TrajPointConfig.class,
    name = "traj_point"
)})
public interface IDataConfig extends Serializable {
DataTypeEnum getDataType();
}
