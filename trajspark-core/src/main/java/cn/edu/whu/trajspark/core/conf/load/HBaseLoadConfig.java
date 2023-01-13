package cn.edu.whu.trajspark.core.conf.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Xu Qi
 * @since 2023/1/9
 */
public class HBaseLoadConfig implements ILoadConfig {

  private String dataSetName;

  @JsonCreator
  public HBaseLoadConfig(@JsonProperty("dataSetName") String dataSetName) {
    this.dataSetName = dataSetName;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  @Override
  public InputType getInputType() {
    return InputType.HBASE;
  }

  @Override
  public String getFsDefaultName() {
    return null;
  }
}
