package cn.edu.whu.trajspark.index;

import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static cn.edu.whu.trajspark.index.conf.Constants.DEFAULT_TIME_PERIOD;
import static cn.edu.whu.trajspark.index.conf.Constants.MAX_TIME_BIN_PRECESION;

/**
 * @author Haocheng Wang
 * Created on 2022/10/2
 */
public class TimeBinIndex {
  private final int g;
  private final TimePeriod timePeriod;
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
  private static Logger logger = LoggerFactory.getLogger(TimeBinIndex.class);

  public TimeBinIndex() {
    g = MAX_TIME_BIN_PRECESION;
    timePeriod = DEFAULT_TIME_PERIOD;
  }

  public TimeBinIndex(int g, TimePeriod timePeriod) {
    if (g > MAX_TIME_BIN_PRECESION) {
      logger.error("Only support time bin precision lower or equal than {}," +
          " but found precision is {}", MAX_TIME_BIN_PRECESION, g);
    }
    this.g = g;
    this.timePeriod = timePeriod;
  }

  public TimeBin getTrajectoryTimeBin(TrajFeatures features) {
    ZonedDateTime zonedDateTime = features.getStartTime();
    return DateToBinnedTime(zonedDateTime);
  }

  public TimeBin EpochSecondToBinnedTime(long time) {
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC);
    return DateToBinnedTime(zonedDateTime);
  }

  public TimeBin DateToBinnedTime(ZonedDateTime zonedDateTime) {
    long binId = timePeriod.getChronoUnit().between(Epoch, zonedDateTime);
    return new TimeBin(binId, timePeriod);
  }

  private long getIndexInner(ZonedDateTime start, ZonedDateTime end) {
    // 1.根据start获取bin num
    // 2. start end归一化
    // 3. 计算SequenceCode长度
    // 4. 得到SequenceCode并转为0-1序列
    return 0;
  }


  public static ZonedDateTime TimeBinToDate(TimeBin binnedTime) {
    return null;
  }

  public int TimeToBin(long time) {
    return 0;
  }

  public int DateToBin(ZonedDateTime zonedDateTime) {
    return 0;
  }

}
