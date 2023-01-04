package cn.edu.whu.trajspark.core.conf.process.noisefilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/11/15
 **/
public class UserDefinedFilterConfig implements IFilterConfig {
  private List<IFilterConfig> filterConfigList;

  @JsonCreator
  public UserDefinedFilterConfig(
      @JsonProperty("filterConfigList") List<IFilterConfig> filterConfigList) {
    this.filterConfigList = filterConfigList;
  }

  public List<IFilterConfig> getFilterConfigList() {
    return filterConfigList;
  }

  public void setFilterConfigList(
      List<IFilterConfig> filterConfigList) {
    this.filterConfigList = filterConfigList;
  }

  @Override
  public FilterEnum getFilterType() {
    return FilterEnum.USERDEFINED_FILTER;
  }
}
