package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Lynn Lee
 * @date 2022/11/4
 **/
public enum FileMode {
  MULTI_FILE("multi_file"), // 用于读取多个文件，且每个文件内只有一条轨迹
  SINGLE_FILE("single_file"), // 用于读取单个大文件，文件内包含多条轨迹
  MULTI_SINGLE_FILE("multi_single_file"); // 用于读取多个小文件，每个文件内包含多条轨迹

  private final String mode;

  FileMode(String mode) {
    this.mode = mode;
  }

  @JsonValue
  public String getMode() {
    return this.mode;
  }
}
