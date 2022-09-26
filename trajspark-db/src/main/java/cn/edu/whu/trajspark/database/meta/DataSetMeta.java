package cn.edu.whu.trajspark.database.meta;

import org.apache.hadoop.hbase.client.Table;

import java.util.Map;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class DataSetMeta {
  String dataSetName;
  Map<Table, TableMeta> dataTables;
}
