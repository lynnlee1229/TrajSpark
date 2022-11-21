package cn.edu.whu.trajspark.coding.poscode;

import java.util.Objects;

/**
 * @author Haocheng Wang
 * Created on 2022/11/7
 */
public class QuadID {

  public static QuadID LEFT_BOTTOM_ID = new QuadID(0);
  public static QuadID RIGHT_BOTTOM_ID = new QuadID(1);
  public static QuadID LEFT_UPPER_ID = new QuadID(2);
  public static QuadID RIGHT_UPPER_ID = new QuadID(3);

  int quadID;

  public QuadID(int quadID) {
    this.quadID = quadID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QuadID quadID1 = (QuadID) o;
    return quadID == quadID1.quadID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(quadID);
  }
}
