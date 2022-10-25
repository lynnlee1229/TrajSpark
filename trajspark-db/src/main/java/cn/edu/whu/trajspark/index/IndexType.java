package cn.edu.whu.trajspark.index;

import java.io.Serializable;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public enum IndexType implements Serializable {
  // spatial only
  XZ2(0),
  XZ2Plus(1),
  // Concatenate temporal index before spatial index
  TXZ2(2),
  // Concatenate spatial index before temporal index
  XZ2T(3),
  // Index value will be car ids
  OBJECT_ID(4),
  T(5);

  int id;

  IndexType(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }
}
