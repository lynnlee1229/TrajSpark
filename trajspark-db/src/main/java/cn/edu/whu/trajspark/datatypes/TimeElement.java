package cn.edu.whu.trajspark.datatypes;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/10/13
 */
public class TimeElement implements Serializable {

  private double timeStart;
  private double timeEnd;
  private double timeExtend;
  private double length;

  public TimeElement(double timeStart, double timeEnd) {
    this.timeStart = timeStart;
    this.timeEnd = timeEnd;
    this.timeExtend = 2 * timeEnd - timeStart;
    this.length = timeEnd - timeStart;
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

  public double getLength() {
    return length;
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

  public Boolean isExContained(TimeLine timeLine) {
    return timeStart >= timeLine.getReTimeStart() && timeExtend <= timeLine.getReTimeEnd();
  }

  public Boolean isExOverlaps(TimeLine timeLine) {
    return timeStart <= timeLine.getReTimeEnd() && timeExtend >= timeLine.getReTimeStart();
  }

  public Boolean isContained(TimeLine timeLine) {
    return timeStart >= timeLine.getReTimeStart() && timeEnd <= timeLine.getReTimeEnd();
  }

  public Boolean isOverlaps(TimeLine timeLine) {
    return timeStart <= timeLine.getReTimeEnd() && timeEnd >= timeLine.getReTimeStart();
  }


  public List<TimeElement> getChildren() {
    double timeCenter =  (timeStart + timeEnd) / 2.0;
    ArrayList<TimeElement> timeElements = new ArrayList<>(2);
    timeElements.add(new TimeElement(timeStart, timeCenter));
    timeElements.add(new TimeElement(timeCenter, timeEnd));
    return timeElements;
  }

  public TimeElement getExtElement() {
    return new TimeElement(this.timeEnd,  this.timeEnd * 2 - this.timeStart);
  }

  /**
   * 获取扩展网格与查询Bound相交的Bound
   */
  public TimeLine getExtOverlappedTimeLine(List<TimeLine> timeLineList) {
    double timeStart = 0;
    double timeEnd = 0;
    for (TimeLine timeLine : timeLineList) {
      if (this.isExOverlaps(timeLine) || this.isExContained(timeLine)) {
        timeStart = Math.max(timeLine.getReTimeStart(), this.timeStart);
        timeEnd = Math.min(timeLine.getReTimeEnd(), this.timeExtend);
      }
    }
    return new TimeLine(timeStart, timeEnd);
  }
  @Override
  public String toString() {
    return "TimeElement{" + "timeStart=" + timeStart + ", timeEnd=" + timeEnd + ", timeExtend="
        + timeExtend + '}';
  }
}
