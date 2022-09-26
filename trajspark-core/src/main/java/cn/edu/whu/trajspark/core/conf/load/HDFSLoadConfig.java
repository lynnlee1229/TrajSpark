package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class HDFSLoadConfig implements ILoadConfig {
  private String master;
  private String fsDefaultName;
  private String location;
  private FileMode fileMode;
  private int partNum;

  private String splitter;

  @JsonCreator
  public HDFSLoadConfig(@JsonProperty("master") String master,
                        @JsonProperty("fsDefaultName") String fsDefaultName,
                        @JsonProperty("location") String location,
                        @JsonProperty("fileMode") FileMode fileMode,
                        @JsonProperty("partNum") @JsonInclude(JsonInclude.Include.NON_NULL)
                        int partNum,
                        @JsonProperty("splitter") String splitter) {
    this.master = master;
    this.fsDefaultName = fsDefaultName;
    this.location = location;
    this.fileMode = fileMode;
    this.partNum = partNum;
    this.splitter = splitter;
  }

  public HDFSLoadConfig() {
  }


  public String getMaster() {
    return this.master;
  }

  public int getPartNum() {
    return this.partNum == 0 ? 1 : this.partNum;
  }

  public String getFsDefaultName() {
    return this.fsDefaultName;
  }

  public String getLocation() {
    return this.location;
  }

  public FileMode getFileMode() {
    return this.fileMode;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public ILoadConfig.InputType getInputType() {
    return InputType.HDFS;
  }

  public String getSplitter() {
    return this.splitter;
  }

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
}
