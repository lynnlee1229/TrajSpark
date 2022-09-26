package cn.edu.whu.trajspark.database.table;

import org.apache.hadoop.hbase.client.Table;

/**
 * 主索引表, key为轨迹时空id, value为轨迹序列.
 *
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class MainIndexTable extends TrajectoryTable {

  public MainIndexTable(Table table) {
    super(table);
  }
}
