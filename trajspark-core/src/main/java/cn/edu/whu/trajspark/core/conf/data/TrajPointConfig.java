package cn.edu.whu.trajspark.core.conf.data;

import cn.edu.whu.trajspark.core.common.field.Field;
import cn.edu.whu.trajspark.core.enums.BasicDataTypeEnum;
import cn.edu.whu.trajspark.core.enums.DataTypeEnum;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
public class TrajPointConfig implements IDataConfig {

  Field pointId;

  Field lat;

  Field lng;

  Field time;

  List<Mapping> trajPointMetas;

  public TrajPointConfig() {
    this.pointId = new Field("traj_point_id", BasicDataTypeEnum.STRING);
    this.lat = new Field("lat", BasicDataTypeEnum.DOUBLE);
    this.lng = new Field("lng", BasicDataTypeEnum.DOUBLE);
    this.time = new Field("time", BasicDataTypeEnum.DATE);
  }

  public Field getPointId() {
    return this.pointId;
  }

  public Field getLat() {
    return this.lat;
  }

  public Field getLng() {
    return this.lng;
  }

  public Field getTime() {
    return this.time;
  }

  public List<Mapping> getTrajPointMetas() {
    return this.trajPointMetas;
  }

  public DataTypeEnum getDataType() {
    return DataTypeEnum.TRAJ_POINT;
  }
}
