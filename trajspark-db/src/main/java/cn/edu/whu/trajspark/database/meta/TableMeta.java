package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.coding.CodingStrategy;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class TableMeta {

  TableType tableType;

  CodingStrategy codingStrategy;

  String tableName;

  String dataSetName;

  DataSetMeta dataSetMeta;

  public enum TableType{
    MAIN_INDEX,
    SECONDARY_INDEX;
  }
}
