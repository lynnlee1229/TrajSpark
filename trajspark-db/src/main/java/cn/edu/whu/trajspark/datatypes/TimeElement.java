package cn.edu.whu.trajspark.datatypes;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/10/13
 */
public class TimeElement {

  private double timeStart;
  private double timeEnd;
  private double timeExtend;

  public TimeElement(double timeStart, double timeEnd) {
    this.timeStart = timeStart;
    this.timeEnd = timeEnd;
    this.timeExtend = 2 * timeEnd - timeStart;
  }

  public double getTimeStart() {
    return timeStart;
  }

  public double getTimeEnd() {
    return timeEnd;
  }

  public double getTimeExtend() {
    return timeExtend;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeElement that = (TimeElement) o;
    return timeStart == that.timeStart && timeEnd == that.timeEnd && timeExtend == that.timeExtend;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeStart, timeEnd, timeExtend);
  }

  public Boolean isContained(TimeLine timeLine) {
    return timeStart >= timeLine.getReTimeStart() && timeExtend <= timeLine.getReTimeEnd();
  }

  public Boolean isOverlaps(TimeLine timeLine) {
    return timeStart <= timeLine.getReTimeEnd() && timeExtend >= timeLine.getReTimeStart();
  }

  public List<TimeElement> getChildren() {
    double timeCenter =  (timeStart + timeEnd) / 2.0;
    ArrayList<TimeElement> timeElements = new ArrayList<>(2);
    timeElements.add(new TimeElement(timeStart, timeCenter));
    timeElements.add(new TimeElement(timeCenter, timeEnd));
    return timeElements;
  }

  @Override
  public String toString() {
    return "TimeElement{" + "timeStart=" + timeStart + ", timeEnd=" + timeEnd + ", timeExtend="
        + timeExtend + '}';
  }
}
