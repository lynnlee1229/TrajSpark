package cn.edu.whu.trajspark.index;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public enum IndexType implements Serializable {
  // spatial only
  XZ2(0);

  int id;

  public static List<IndexType> spatialIndexTypes() {
    return Arrays.asList(XZ2);
  }

  IndexType(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
