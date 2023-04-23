package cn.edu.whu.trajspark.query.basic.developing;

import cn.edu.whu.trajspark.index.IndexType;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;

import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2023/2/17
 */
public class BasicQueryCondition {
  private SpatialQueryCondition sqc;
  private TemporalQueryCondition tqc;
  private IDQueryCondition idqc;

  public void setStc(SpatialQueryCondition sqc) {
    this.sqc = sqc;
  }

  public void setTqc(TemporalQueryCondition tqc) {
    this.tqc = tqc;
  }

  public void setIdqc(IDQueryCondition idqc) {
    this.idqc = idqc;
  }

  /**
   * TODO: 根据三个维度的查询条件，选择可用的索引类型
   * @return
   */
  public List<IndexType> usableIndexTypes() {
    // 仅时间: TXZ2  T
    // 仅空间: XZ2T XZ2
    // 仅ID: ID-T
    // 时间+ID: ID-T TXZ2 T
    // 空间+ID: XZ2T XZ2
    // 时间+空间(+ID): TXZ2 XZ2T
    return null;
  }

  public static class Builder {
    BasicQueryCondition bqc;

    public Builder() {
      this.bqc = new BasicQueryCondition();
    }

    public Builder setTemporalQueryCondition(TemporalQueryCondition tqc) {
      this.bqc.setTqc(tqc);
      return this;
    }

    public Builder setSpatialQueryCondition(SpatialQueryCondition sqc) {
      this.bqc.setStc(sqc);
      return this;
    }

    public Builder setIDQueryCondition(IDQueryCondition idQueryCondition) {
      this.bqc.setIdqc(idQueryCondition);
      return this;
    }

    public BasicQueryCondition build() {
      return bqc;
    }
  }
}
