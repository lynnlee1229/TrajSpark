package cn.edu.whu.trajspark.query.condition;

import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import java.util.List;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class TemporalQueryCondition {

  private TimeLine queryWindow;
  private List<TimeLine> queryWindows;
  private final TemporalQueryType temporalQueryType;

  public TemporalQueryCondition(TimeLine queryWindow,
      TemporalQueryType temporalQueryType) {
    this.queryWindow = queryWindow;
    this.temporalQueryType = temporalQueryType;
  }

  public TemporalQueryCondition(List<TimeLine> queryWindows,
      TemporalQueryType temporalQueryType) {
    this.queryWindows = queryWindows;
    this.temporalQueryType = temporalQueryType;
  }

  public TimeLine getQueryWindow() {
    return queryWindow;
  }

  public TemporalQueryType getTemporalQueryType() {
    return temporalQueryType;
  }

  public List<TimeLine> getQueryWindows() {
    return queryWindows;
  }
}
