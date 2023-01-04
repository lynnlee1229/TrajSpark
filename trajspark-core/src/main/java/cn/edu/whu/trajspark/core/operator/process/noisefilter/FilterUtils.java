package cn.edu.whu.trajspark.core.operator.process.noisefilter;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Lynn Lee
 * @date 2022/11/13
 **/
public class FilterUtils {

  public static List<TrajPoint> sortPointList(List<TrajPoint> rawPointList) {
    rawPointList.sort(Comparator.comparing(TrajPoint::getTimestamp));
    return rawPointList;
  }

  /**
   * 时间去重
   *
   * @param rawPointList
   * @return
   */
  public static List<TrajPoint> dropTimeDuplication(List<TrajPoint> rawPointList) {
    Set<TrajPoint> tmpSet = new TreeSet<>(Comparator.comparing(TrajPoint::getTimestamp));
    tmpSet.addAll(rawPointList);
    return new ArrayList<>(tmpSet);
  }

  /**
   * 空间去重
   *
   * @param rawPointList
   * @return
   */
  public static List<TrajPoint> dropLocDuplication(List<TrajPoint> rawPointList) {
    List<TrajPoint> cleanList = new ArrayList<>();
    int i = 0, j, k;
    while (i < rawPointList.size() - 1) {
      j = i + 1;
      while (j < rawPointList.size()) {
        if (isSameLoc(rawPointList.get(i), rawPointList.get(j))) {
          ++j;
        } else {
          break;
        }
      }
      cleanList.add(rawPointList.get(i));
      if (j == i + 1) {
        ++i;
      } else {
        cleanList.add(rawPointList.get(j - 1));
        i = j;
      }
    }
    return cleanList;
  }

  public static boolean isSameLoc(TrajPoint p1, TrajPoint p2) {
    return p1.getLng() == p2.getLng() && p1.getLat() == p2.getLat();
  }
}
