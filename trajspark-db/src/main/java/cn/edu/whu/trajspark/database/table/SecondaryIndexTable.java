package cn.edu.whu.trajspark.database.table;

import org.apache.hadoop.hbase.client.Table;

/**
 * 辅助索引表, 仅存储轨迹ID
 *
 * @author Haocheng Wang Created on 2022/9/28
 */
public class SecondaryIndexTable extends TrajectoryTable {

  public SecondaryIndexTable(Table table) {
    super(table);
  }
}
