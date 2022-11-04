package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Lynn Lee
 * @date 2022/11/4
 **/
public enum FileMode {
  MULTI_FILE("multi_file"),
  SINGLE_FILE("single_file");

  private String mode;

  FileMode(String mode) {
    this.mode = mode;
  }

  @JsonValue
  public String getMode() {
    return this.mode;
  }
}
