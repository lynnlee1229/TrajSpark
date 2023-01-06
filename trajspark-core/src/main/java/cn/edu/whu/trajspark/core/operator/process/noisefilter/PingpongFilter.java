package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.base.point.TrajPoint;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.PingpongFilterConfig;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author Lynn Lee
 * @date 2022/10/28
 **/
public class PingpongFilter implements IFilter {

  private String baseStationIndex;
  private double maxPingpongTime;
  private double minTrajLength;

  /**
   * 乒乓效应去除
   *
   * @param baseStationIndex 基站ID索引名
   * @param maxPingpongTime  时间阈值，在该阈值内"A-B-A"被视作乒乓效应
   * @param minTrajLength    最小轨迹长度，单位：km
   */
  public PingpongFilter(String baseStationIndex, double maxPingpongTime, double minTrajLength) {
    this.baseStationIndex = baseStationIndex;
    this.maxPingpongTime = maxPingpongTime;
    this.minTrajLength = minTrajLength;
  }

  /**
   * 通过配置文件构造
   *
   * @param config
   */
  public PingpongFilter(PingpongFilterConfig config) {
    this.baseStationIndex = config.getBaseStationIndex();
    this.maxPingpongTime = config.getMaxPingpongTime();
    this.minTrajLength = config.getMinTrajLength();
  }

  @Override
  public Trajectory filterFunction(Trajectory rawTrajectory) {
    // 时空去重
    List<TrajPoint> tmpPointList = FilterUtils.sortPointList(rawTrajectory.getPointList());
    tmpPointList = FilterUtils.dropTimeDuplication(tmpPointList);
    // 去除乒乓效应
    List<TrajPoint> filterABA = filterABA(tmpPointList);
    List<TrajPoint> cleanPointList = filterABCA(filterABA);
    // 去除乒乓效应完成
    Trajectory cleanedTrajtroy =
        new Trajectory(rawTrajectory.getTrajectoryID(), rawTrajectory.getObjectID(),
            cleanPointList, rawTrajectory.getExtendedValues());
    return cleanedTrajtroy.getTrajectoryFeatures().getLen() > minTrajLength ? cleanedTrajtroy
        : null;
  }

  /**
   * 过滤ABA类型乒乓噪声
   *
   * @param rawList
   * @return
   */
  private List<TrajPoint> filterABA(List<TrajPoint> rawList) {
    // 基站信息符合A-B-A且小于时间阈值被认为发生了乒乓效应
    for (int i = 0; i < rawList.size() - 2; ++i) {
      // 循环判断 i ，i+1，i+2是否发生乒乓效应
      TrajPoint tp0 = rawList.get(i);
      TrajPoint tp1 = rawList.get(i + 1);
      TrajPoint tp2 = rawList.get(i + 2);
      String bs0, bs1, bs2;
      // 数据不含基站ID列，则根据位置拼接生成
      if (tp0.getExtendedValue(baseStationIndex) == null) {
        bs0 = genBSID(tp0);
        bs1 = genBSID(tp1);
        bs2 = genBSID(tp2);
      } else {
        bs0 = (String) tp0.getExtendedValue(baseStationIndex);
        bs1 = (String) tp1.getExtendedValue(baseStationIndex);
        bs2 = (String) tp2.getExtendedValue(baseStationIndex);
      }
      if (bs2.equals(bs0) && !bs1.equals(bs0)) {
        // 基站表现为A-B-A，计算时间tp0和tp2的时间差
        double deltaT = (double) ChronoUnit.SECONDS.between(tp2.getTimestamp(), tp0.getTimestamp());
        if (deltaT <= maxPingpongTime) {
          // 时间差小于阈值，认为发生了乒乓效应
          // 删除
          rawList.remove(i + 1);
        }
      }
    }
    return rawList;
  }

  /**
   * 过滤ABCA形式乒乓噪声
   *
   * @param rawList
   * @return
   */
  private List<TrajPoint> filterABCA(List<TrajPoint> rawList) {
    // 基站信息符合A-B-C-A且小于时间阈值被认为发生了乒乓效应
    for (int i = 0; i < rawList.size() - 3; ++i) {
      // 循环判断 i ，i+1，i+2,i+3是否发生乒乓效应
      TrajPoint tp0 = rawList.get(i);
      TrajPoint tp1 = rawList.get(i + 1);
      TrajPoint tp2 = rawList.get(i + 2);
      TrajPoint tp3 = rawList.get(i + 3);
      String bs0, bs1, bs2, bs3;
      // 数据不含基站ID列，则根据位置拼接生成
      if (tp0.getExtendedValue(baseStationIndex) == null) {
        bs0 = genBSID(tp0);
        bs1 = genBSID(tp1);
        bs2 = genBSID(tp2);
        bs3 = genBSID(tp3);
      } else {
        bs0 = (String) tp0.getExtendedValue(baseStationIndex);
        bs1 = (String) tp1.getExtendedValue(baseStationIndex);
        bs2 = (String) tp2.getExtendedValue(baseStationIndex);
        bs3 = (String) tp3.getExtendedValue(baseStationIndex);
      }
      if (bs3.equals(bs0) && !bs1.equals(bs0) && !bs2.equals(bs0)) {
        // 基站表现为A-B-C-A，计算时间tp0和tp3的时间差
        double deltaT = (double) ChronoUnit.SECONDS.between(tp3.getTimestamp(), tp0.getTimestamp());
        if (deltaT <= maxPingpongTime) {
          // 时间差小于阈值，认为发生了乒乓效应
          // 删除
          rawList.remove(i + 1);
          rawList.remove(i + 1);
        }
      }
    }
    return rawList;
  }

  private String genBSID(TrajPoint p) {
    return p.getLat() + "#" + p.getLng();
  }

  @Override
  public JavaRDD<Trajectory> filter(JavaRDD<Trajectory> rawTrajectoryRDD) {
    return rawTrajectoryRDD.map(this::filterFunction);
  }
}
