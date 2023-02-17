package cn.edu.whu.trajspark.constant;

/**
 * @author Haocheng Wang
 * Created on 2022/10/23
 */
public class DBConstants {
  // Tables
  public static final String META_TABLE_NAME = "trajspark_db_meta";
  public static final String META_TABLE_COLUMN_FAMILY = "meta";
  public static final String META_TABLE_INDEX_META_QUALIFIER = "index_meta";
  public static final String META_TABLE_MAIN_TABLE_META_QUALIFIER = "main_table_index_meta";
  public static final String META_TABLE_DESC_QUALIFIER = "desc";

  // INDEX TABLE
  public static final String DATA_TABLE_SUFFIX = "_data";
  public static String INDEX_TABLE_CF = "cf0";

  // INDEX TABLE COLUMNS
  public static String BOUNDING_BOX = "BB";
  public static String ORIGIN_INFO = "OI";
  public static String DESTINATION_INFO = "DI";
  public static String SPEED = "SPD";
  public static String DIRECTION = "DIR";
  public static String DISTANCE_LENGTH = "DIST_LEN";
  public static String TIME_LENGTH = "TIME_LEN";
  public static String POINT_LIST = "POINT_LIST";
  public static String EXTRA_INFO = "EXTRA_INFO";
  public static String POINTER = "PTR";

  // Connection
  public static final String OPEN_CONNECTION_FAILED = "Cannot connect to data base.";
  public static final String CLOSE_CONNECTION_FAILED = "Close connection failed.";

  // Initial
  public static final String INITIAL_FAILED = "Initial failed.";
}
