package cn.edu.whu.trajspark.index;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public enum IndexType {
  // spatial only
  XZ2,
  // Concatenate temporal index before spatial index
  TXZ2,
  // Concatenate spatial index before temporal index
  XZ2T,
  // Index value will be car ids
  OBJECT_ID
}
