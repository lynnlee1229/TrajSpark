package cn.edu.whu.trajspark.constant;

import org.apache.hadoop.hbase.util.Bytes;

import static cn.edu.whu.trajspark.base.trajectory.Trajectory.Schema.*;

/**
 * @author Haocheng Wang
 * Created on 2022/10/23
 */
public class DBConstants {
  // Tables
  public static final String META_TABLE_NAME = "trajspark_db_meta";
  public static final String META_TABLE_COLUMN_FAMILY = "meta";
  public static final String META_TABLE_INDEX_META_QUALIFIER = "index_meta";
  public static final String META_TABLE_CORE_INDEX_META_QUALIFIER = "main_table_index_meta";
  public static final String META_TABLE_DESC_QUALIFIER = "desc";

  // INDEX TABLE COLUMNS
  public static final String DATA_TABLE_SUFFIX = "_data";
  public static String INDEX_TABLE_CF = "cf0";
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes(INDEX_TABLE_CF);
  public static final byte[] TRAJECTORY_ID_QUALIFIER = Bytes.toBytes(TRAJECTORY_ID);
  public static final byte[] OBJECT_ID_QUALIFIER = Bytes.toBytes(OBJECT_ID);
  public static final byte[] MBR_QUALIFIER = Bytes.toBytes(MBR);
  public static final byte[] START_POINT_QUALIFIER = Bytes.toBytes(START_POSITION);
  public static final byte[] END_POINT_QUALIFIER = Bytes.toBytes(END_POSITION);
  public static final byte[] TRAJ_POINTS_QUALIFIER = Bytes.toBytes(TRAJ_POINTS);
  public static final byte[] SIGNATURE_QUALIFIER = Bytes.toBytes(SIGNATURE);
  public static final byte[] PTR_QUALIFIER = Bytes.toBytes(PTR);

  // Bulk load
  public static final String BULK_LOAD_TEMP_FILE_PATH = "import.file.output.path";
  public static final String PROCESS_INPUT_CONF_KEY = "import.process.input.path";

  // Connection
  public static final String OPEN_CONNECTION_FAILED = "Cannot connect to data base.";
  public static final String CLOSE_CONNECTION_FAILED = "Close connection failed.";

  // Initial
  public static final String INITIAL_FAILED = "Initial failed.";
}
