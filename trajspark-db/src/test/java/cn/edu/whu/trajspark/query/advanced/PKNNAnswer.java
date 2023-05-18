package cn.edu.whu.trajspark.query.advanced;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.base.util.GeoUtils;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author Haocheng Wang
 * Created on 2023/2/17
 */
public class PKNNAnswer {

  private static Logger logger = LoggerFactory.getLogger(PKNNAnswer.class);

  double maxDistKM;
  BasePoint queryPoint;
  TemporalQueryCondition tqc;
  int k;
  List<Trajectory> data;

  public PKNNAnswer(double maxDistKM, BasePoint queryPoint, int k, List<Trajectory> data) {
    this.maxDistKM = maxDistKM;
    this.queryPoint = queryPoint;
    this.k = k;
    this.data = data;
  }

  public PKNNAnswer(double maxDistKM, BasePoint queryPoint, TemporalQueryCondition tqc, int k, List<Trajectory> data) {
    this.maxDistKM = maxDistKM;
    this.queryPoint = queryPoint;
    this.tqc = tqc;
    this.k = k;
    this.data = data;
  }

  public List<Trajectory> execute() {
    STRtree stRtree = new STRtree();
    for (Trajectory t : data) {
      stRtree.insert(t.getLineString().getEnvelopeInternal(), t);
    }

    int stage = 1;
    try {
      List<Trajectory> result = new LinkedList<>();
      while (result.size() < k && getSearchRadiusKM(stage) <= maxDistKM) {
        logger.info("KNN query stage {}, current search radius is {}.", stage, getSearchRadiusKM(stage));
        Geometry queryEnvelop = queryPoint.buffer(GeoUtils.getDegreeFromKm(getSearchRadiusKM(stage)));
        SpatialQueryCondition sqc = new SpatialQueryCondition(queryEnvelop, SpatialQueryCondition.SpatialQueryType.INTERSECT);
        SpatialTemporalQueryCondition stqc = new SpatialTemporalQueryCondition(sqc, tqc);
        result = queryOnRTree(stRtree, stqc);
        logger.info("The count of trajectories get by [stage {}, search radius {}km] is {}", stage, getSearchRadiusKM(stage), result.size());
        stage++;
      }
      if (result.size() < k) {
        logger.info("Reach max search radius {} at stage {}, radius {}, and only got {} of {}.",
            maxDistKM, stage, getSearchRadiusKM(stage), result.size(), k);
      }
      // 大顶堆求K个最小值
      PriorityQueue<KNNTrajectory> pq = new PriorityQueue<>(k, (o1, o2) -> {
        double dist1 = o1.getDistanceToPoint();
        double dist2 = o2.getDistanceToPoint();
        return Double.compare(dist2, dist1);
      });
      addToHeap(result, pq);
      return heapToResultList(pq);
    } catch (Exception e) {
      logger.error("KNN query failed at stage {}, search radius {}", stage, getSearchRadiusKM(stage), e);
      return new LinkedList<>();
    }
  }

  private double getSearchRadiusKM(int stage) {
    return 1 * Math.pow(Math.sqrt(2), stage - 1);
  }


  /**
   * 大顶堆求k个最小值
   */
  private void addToHeap(List<Trajectory> trajList, PriorityQueue<KNNTrajectory> pq) {
    for (Trajectory t : trajList) {
      KNNTrajectory kt = new KNNTrajectory(t, queryPoint);
      if (pq.size() < k) {
        pq.offer(kt);
      } else if (pq.peek().getDistanceToPoint() > kt.getDistanceToPoint()) {
        pq.poll();
        pq.offer(kt);
      }
    }
  }

  private List<Trajectory> heapToResultList(PriorityQueue<KNNTrajectory> pq) {
    List<Trajectory> result = new LinkedList<>();
    while (!pq.isEmpty()) {
      result.add(pq.poll().getTrajectory());
    }
    return result;
  }

  private List<Trajectory> queryOnRTree(STRtree stRtree, SpatialTemporalQueryCondition stqc) {
    List<Trajectory> rtResult = stRtree.query(stqc.getSpatialQueryCondition().getQueryWindow());
    if (stqc.getTemporalQueryCondition() == null) {
      return rtResult;
    }
    List<Trajectory> result = new LinkedList<>();
    // check temporal attr
    for (Trajectory t : rtResult) {
      TimeLine timeLine = new TimeLine(t.getTrajectoryFeatures().getStartTime(),
          t.getTrajectoryFeatures().getEndTime());
      if (stqc.getTemporalQueryCondition().validate(timeLine)) {
        result.add(t);
      }
    }
    return result;
  }
}
