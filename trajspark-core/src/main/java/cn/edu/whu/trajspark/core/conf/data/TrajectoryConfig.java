package cn.edu.whu.trajspark.core.conf.data;

import cn.edu.whu.trajspark.base.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.field.Field;
import cn.edu.whu.trajspark.core.enums.BasicDataTypeEnum;
import cn.edu.whu.trajspark.core.enums.DataTypeEnum;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/9/17
 **/
public class TrajectoryConfig implements IDataConfig {
  @JsonProperty
  private Field trajId;
  @JsonProperty
  private Field objectId;
  @JsonProperty
  private Field trajLog;
  @JsonProperty
  private TrajPointConfig trajPointConfig;
  @JsonProperty
  private TrajFeatures trajectoryFeatures;
  @JsonProperty
  private List<Mapping> trajectoryMetas;
  @JsonProperty
  private String attrIndexable;
  @JsonProperty
  private String pointIndex;
  @JsonProperty
  private String listIndex;

  public TrajectoryConfig() {
    this.trajId = new Field("trajectory_id", BasicDataTypeEnum.STRING);
    this.objectId = new Field("object_id", BasicDataTypeEnum.STRING);
    this.trajLog = new Field("traj_list", BasicDataTypeEnum.STRING);
    this.trajPointConfig = new TrajPointConfig();
    this.pointIndex = "";
    this.listIndex = "";
  }

  public Field getTrajId() {
    return this.trajId;
  }

  public Field getObjectId() {
    return this.objectId;
  }

  public TrajFeatures getTrajectoryFeatures() {
    return this.trajectoryFeatures;
  }

  public List<Mapping> getTrajectoryMetas() {
    return this.trajectoryMetas;
  }

  public TrajPointConfig getTrajPointConfig() {
    return trajPointConfig;
  }

  public Field getTrajLog() {
    return this.trajLog;
  }

  public DataTypeEnum getDataType() {
    return DataTypeEnum.TRAJECTORY;
  }

  public String isIndexable() {
    return this.attrIndexable;
  }

  public String getPointIndex() {
    return this.pointIndex;
  }

  public String getListIndex() {
    return this.listIndex;
  }
}
