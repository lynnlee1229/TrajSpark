package cn.edu.whu.trajspark.datatypes;

/**
 * @author Xu Qi
 * @since 2022/10/12
 */
public enum TemporalQueryType {
  /**
   * Query all data that is totally contained in query window.
   */
  INCLUDE,
  /**
   * Query all data that may INTERSECT with query window.
   */
  OVERLAP;
}
