package cn.edu.whu.trajspark.coding;

import static cn.edu.whu.trajspark.coding.conf.Constants.MAX_TIME_BIN_PRECISION;
import static cn.edu.whu.trajspark.index.spatial.SpatialIndexStrategyTest.getExampleTrajectory;
import static org.junit.jupiter.api.Assertions.*;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeBin;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.time.TimeIndexStrategy;
import java.util.List;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;


/**
 * @author Xu Qi
 * @since 2022/10/8
 */
class TimeLineCodingTest extends TestCase {

  @Test
  public void getIndex() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    TimeLineCoding timeLineCoding = new TimeLineCoding();
    TimeIndexStrategy timeIndexStrategy = new TimeIndexStrategy(timeLineCoding, (short) 0);
    ByteArray index = timeIndexStrategy.index(exampleTrajectory);
    TimeLine timeLineRange = timeIndexStrategy.getTimeLineRange(index);
    System.out.println("ByteArray: " + index);
    System.out.println(
        "timeStart: " + exampleTrajectory.getTrajectoryFeatures().getStartTime() + "timeEnd: "
            + exampleTrajectory.getTrajectoryFeatures().getEndTime());
    System.out.println(timeLineRange);
    short timeBinVal = timeIndexStrategy.getTimeBinVal(index);
    long timeCodingVal = timeIndexStrategy.getTimeCodingVal(index);
    List<Integer> sequenceCode = timeLineCoding.getSequenceCode(timeCodingVal);
    long coding = 0L;
    for (int i = 0; i < sequenceCode.size(); i++) {
      coding += 1L + sequenceCode.get(i) * ((long) Math.pow(2, MAX_TIME_BIN_PRECISION - i) - 1L);
    }
    System.out.println("timeBinVal: " + timeBinVal);
    System.out.println("timeCodingVal: " + timeCodingVal);
    System.out.println("coding: " + coding);
  }
}