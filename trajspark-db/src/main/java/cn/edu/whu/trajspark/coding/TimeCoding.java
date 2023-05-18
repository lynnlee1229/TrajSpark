package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author Haocheng Wang Created on 2022/10/4
 */
public interface TimeCoding extends Serializable {

  long getIndex(TimeLine timeline);

  TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime);

  TimeLine getXZTElementTimeLine(long coding);

  TimePeriod getTimePeriod();

  long getIndex(ZonedDateTime start, ZonedDateTime end);

  List<CodingRange> ranges(TemporalQueryCondition condition);
}
