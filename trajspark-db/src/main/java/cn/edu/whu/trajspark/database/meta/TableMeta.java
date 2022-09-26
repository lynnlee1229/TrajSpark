package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.coding.Coding;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class TableMeta {

  TableType tableType;

  Coding coding;

  String tableName;

  String dataSetName;

  DataSetMeta dataSetMeta;

  public enum TableType{
    MAIN_INDEX,
    SECONDARY_INDEX;
  }
}
