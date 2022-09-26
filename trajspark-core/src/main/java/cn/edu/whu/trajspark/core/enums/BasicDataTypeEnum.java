package cn.edu.whu.trajspark.core.enums;

import com.fasterxml.jackson.annotation.JsonValue;
import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/16
 **/
public enum BasicDataTypeEnum implements Serializable {
  STRING("String"),
  INT("Integer"),
  LONG("Long"),
  DOUBLE("Double"),
  DATE("Date"),
  TIME_STRING("TimeString"),
  TIME_LONG("TimeLong"),
  TIMESTAMP("Timestamp"),
  LIST("List");

  private String typeName;

  BasicDataTypeEnum(String typeName) {
    this.typeName = typeName;
  }
  @JsonValue
  public final String getType() {
    return this.typeName;
  }
}
