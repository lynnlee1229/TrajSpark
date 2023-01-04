package cn.edu.whu.trajspark.core.conf.process.segmenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/11/15
 **/
public class UserDefinedSegmenterConfig implements ISegmenterConfig {
  private List<ISegmenterConfig> segmenterConfigList;

  @JsonCreator
  public UserDefinedSegmenterConfig(
      @JsonProperty("segmenterConfigList") List<ISegmenterConfig> segmenterConfigList) {
    this.segmenterConfigList = segmenterConfigList;
  }

  public List<ISegmenterConfig> getSegmenterConfigList() {
    return segmenterConfigList;
  }

  public void setSegmenterConfigList(
      List<ISegmenterConfig> segmenterConfigList) {
    this.segmenterConfigList = segmenterConfigList;
  }

  @Override
  public SegmenterEnum getSegmenterType() {
    return SegmenterEnum.USERDEFINED_SEGMENTER;
  }
}
