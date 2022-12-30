package cn.edu.whu.trajspark.query.condition;

/**
 * @author Xu Qi
 * @since 2022/11/30
 */
public class SpatialTemporalQueryCondition {

  private final SpatialQueryCondition spatialQueryCondition;
  private final TemporalQueryCondition temporalQueryCondition;

  public SpatialTemporalQueryCondition(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition) {
    this.spatialQueryCondition = spatialQueryCondition;
    this.temporalQueryCondition = temporalQueryCondition;
  }

  public SpatialQueryCondition getSpatialQueryCondition() {
    return spatialQueryCondition;
  }

  public TemporalQueryCondition getTemporalQueryCondition() {
    return temporalQueryCondition;
  }
  public enum STQueryType {
    /**
     * Query all data that may INTERSECT with query window.
     */
    CONTAIN,
    /**
     * Query all data that is totally contained in query window.
     */
    INTERSECT;
  }
}
