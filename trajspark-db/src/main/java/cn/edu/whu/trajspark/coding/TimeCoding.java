package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.sfcurve.IndexRange;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public interface TimeCoding extends Serializable {

  long getIndex(ZonedDateTime time);

  long getIndex(ZonedDateTime start, ZonedDateTime end);

  List<IndexRange> ranges(TemporalQueryCondition condition);
}
