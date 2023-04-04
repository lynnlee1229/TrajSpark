package cn.edu.whu.trajspark.query.advanced;

import cn.edu.whu.trajspark.base.point.BasePoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.base.util.GeoUtils;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.basic.SpatialTemporalQuery;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * @author Haocheng Wang
 * Created on 2023/2/16
 */
public class PKNNQuery {

  private static Logger logger = LoggerFactory.getLogger(PKNNQuery.class);
  public double BASIC_BUFFER_DISTANCE = 1.0;

  DataSet dataSet;
  IndexTable targetIndexTable;
  int k;
  BasePoint queryPoint;
  TemporalQueryCondition tqc;
  double maxDistKM;

  public PKNNQuery(DataSet dataSet, int k, BasePoint queryPoint, TemporalQueryCondition tqc, double maxDistKM) {
    this.dataSet = dataSet;
    this.k = k;
    this.queryPoint = queryPoint;
    this.tqc = tqc;
    this.maxDistKM = maxDistKM;
  }

  public PKNNQuery(IndexTable targetIndexTable, int k, BasePoint queryPoint, TemporalQueryCondition tqc, double maxDistKM) {
    this.targetIndexTable = targetIndexTable;
    this.k = k;
    this.queryPoint = queryPoint;
    this.tqc = tqc;
    this.maxDistKM = maxDistKM;
  }

  private boolean hasTimeConstrain() {
    return tqc != null;
  }

  private void setupTargetIndexTable() throws IOException, IllegalAccessException {
    if (targetIndexTable == null) {
      IndexMeta indexMeta = findBestIndex();
      if (indexMeta == null) {
        throw new IllegalAccessException("无可用于KNN的索引，暂不支持以全表扫描的形式执行查询。");
      }
      logger.info("KNN will be executed on table: {}", indexMeta.getIndexTableName());
      targetIndexTable = dataSet.getIndexTable(indexMeta);
    }
  }

  private IndexMeta findBestIndex() {
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    // case 1: 无时间约束，找XZ2索引，或XZ2T索引
    if (!hasTimeConstrain()) {
       if (map.containsKey(IndexType.XZ2T)) {
        return IndexMeta.getBestIndexMeta(map.get(IndexType.XZ2T));
      }
    }
    // case 2: 有时间约束，找TXZ2索引
    else {
      if (map.containsKey(IndexType.TXZ2)) {
        return IndexMeta.getBestIndexMeta(map.get(IndexType.TXZ2));
      }
    }
    return null;
  }

  public List<Trajectory> execute() {
    int stage = 1;
    double curSearchDist = BASIC_BUFFER_DISTANCE;
    try {
      setupTargetIndexTable();
      List<Trajectory> result = new LinkedList<>();
      while (result.size() < k) {
        logger.info("KNN query stage {}, current search radius is {}.", stage, curSearchDist);
        Envelope queryEnvelop = queryPoint.buffer(GeoUtils.getDegreeFromKm(curSearchDist)).getEnvelopeInternal();
        SpatialQueryCondition sqc = new SpatialQueryCondition(queryEnvelop, SpatialQueryCondition.SpatialQueryType.INTERSECT);
        SpatialTemporalQueryCondition stqc = new SpatialTemporalQueryCondition(sqc, tqc);
        result = new SpatialTemporalQuery(targetIndexTable, stqc, false).executeQuery();
        logger.info("The count of trajectories get by [stage {}, search radius {}km] is {}", stage, curSearchDist, result.size());
        stage++;
        if (getSearchRadiusKM(result.size(), k, curSearchDist) >= maxDistKM) {
          break;
        } else {
          curSearchDist = getSearchRadiusKM(result.size(), k, curSearchDist);
        }
      }
      if (result.size() < k) {
        logger.info("Reach max search radius {} at stage {}, radius {}, and only got {} of {}.",
            maxDistKM, stage, curSearchDist, result.size(), k);
      }
      PriorityQueue<KNNTrajectory> pq = new PriorityQueue<>(k, (o1, o2) -> {
        double dist1 = o1.getDistanceToPoint();
        double dist2 = o2.getDistanceToPoint();
        return Double.compare(dist2, dist1);
      });
      addToHeap(result, pq);
      return heapToResultList(pq);
    } catch (Exception e) {
      logger.error("KNN query failed at stage {}, search radius {}", stage, curSearchDist, e);
      return new LinkedList<>();
    }
  }

  private double getSearchRadiusKM(int lastGetRecords, int k, double curSearchDist) {
    if (lastGetRecords == 0 || k / lastGetRecords >= 2) {
      return curSearchDist * Math.sqrt(2);
    } else {
      return curSearchDist * Math.sqrt(k / (double) lastGetRecords);
    }
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
}
