package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class StandaloneLoadConfig implements ILoadConfig {
  private String master;
  private String location;
  private FileMode fileMode;
  private int partNum;
  private String splitter;

  @JsonCreator
  public StandaloneLoadConfig(@JsonProperty("master") String master,
                              @JsonProperty("location") String location,
                              @JsonProperty("fileMode") FileMode fileMode,
                              @JsonProperty("partNum") @JsonInclude(JsonInclude.Include.NON_NULL)
                              int partNum,
                              @JsonProperty("splitter") String splitter) {
    this.master = master;
    this.location = location;
    this.fileMode = fileMode;
    this.partNum = partNum;
    this.splitter = splitter;
  }

  public String getMaster() {
    return this.master;
  }

  public int getPartNum() {
    return this.partNum == 0 ? 1 : this.partNum;
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
    return InputType.STANDALONE;
  }

  @Override
  public String getFsDefaultName() {
    return null;
  }

  public String getSplitter() {
    return this.splitter;
  }


}
