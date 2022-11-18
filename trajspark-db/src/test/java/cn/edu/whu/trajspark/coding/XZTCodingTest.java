package cn.edu.whu.trajspark.coding;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_TIME_BIN_PRECISION;
import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.time.TimeIndexStrategy;
import java.util.List;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;


/**
 * @author Xu Qi
 * @since 2022/10/8
 */
class XZTCodingTest extends TestCase {

  @Test
  public void getIndex() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    TimeIndexStrategy timeIndexStrategy = new TimeIndexStrategy(XZTCoding);
    ByteArray index = timeIndexStrategy.index(exampleTrajectory);
    TimeLine timeLineRange = timeIndexStrategy.getTimeLineRange(index);
    System.out.println("ByteArray: " + index);
    System.out.println(
        "timeStart: " + exampleTrajectory.getTrajectoryFeatures().getStartTime() + "timeEnd: "
            + exampleTrajectory.getTrajectoryFeatures().getEndTime());
    System.out.println(timeLineRange);
    short timeBinVal = timeIndexStrategy.getTimeBinVal(index);
    long timeCodingVal = timeIndexStrategy.getTimeCodingVal(index);
    List<Integer> sequenceCode = XZTCoding.getSequenceCode(timeCodingVal);
    long coding = 0L;
    for (int i = 0; i < sequenceCode.size(); i++) {
      coding += 1L + sequenceCode.get(i) * ((long) Math.pow(2, MAX_TIME_BIN_PRECISION - i) - 1L);
    }
    System.out.println("timeBinVal: " + timeBinVal);
    System.out.println("timeCodingVal: " + timeCodingVal);
    System.out.println("coding: " + coding);
    assert timeCodingVal == coding;
  }
}