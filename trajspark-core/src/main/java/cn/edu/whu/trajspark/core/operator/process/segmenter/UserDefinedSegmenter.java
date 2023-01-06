package cn.edu.whu.trajspark.core.operator.process.segmenter;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.segmenter.UserDefinedSegmenterConfig;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/11/15
 **/
public class UserDefinedSegmenter implements ISegmenter {
  private List<ISegmenter> segmenterList;

  public UserDefinedSegmenter(List<ISegmenter> segmenterList) {
    this.segmenterList = segmenterList;
  }

  public UserDefinedSegmenter(UserDefinedSegmenterConfig config) {
    this.segmenterList =
        config.getSegmenterConfigList().stream().map(ISegmenter::getSegmenter).collect(
            Collectors.toList());
  }

  @Override
  public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
    String rawTid = rawTrajectory.getTrajectoryID();
    Queue<Trajectory> segList = new LinkedList<>();
    Queue<Trajectory> tmpList = new LinkedList<>();
    segList.add(rawTrajectory);
    for (ISegmenter iSegmenter : segmenterList) {
      while (!segList.isEmpty()) {
        Trajectory trajectory = segList.poll();
        trajectory.setTrajectoryID(rawTid);
        tmpList.addAll(iSegmenter.segmentFunction(trajectory));
      }
      segList.addAll(tmpList);
      tmpList.clear();
    }
    return new ArrayList<>(segList);
  }

  @Override
  public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator());
  }
}
