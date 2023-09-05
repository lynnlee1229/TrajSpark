package cn.edu.whu.trajspark.core.enums;

/**
 * @author Lynn Lee
 * @date 2022/9/22
 **/
public enum StoreSchemaEnum {
  POINT_BASED_TRAJECTORY("trajectory_point"),
  LIST_BASED_TRAJECTORY("trajectory_list"),
  STAY_POINT("stay_point"),
  POINT_BASED_TRAJECTORY_SLOWPUT("trajectory_point_slowput");

  private String storeSchema;

  StoreSchemaEnum(String storeSchema) {
    this.storeSchema = storeSchema;
  }

  public final String getType() {
    return this.storeSchema;
  }

  public static class Constants {
    public Constants() {
    }
  }
}
