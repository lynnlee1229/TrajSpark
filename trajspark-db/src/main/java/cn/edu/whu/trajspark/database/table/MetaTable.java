package cn.edu.whu.trajspark.database.table;

import cn.edu.whu.trajspark.base.util.SerializerUtils;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.index.IndexType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.whu.trajspark.constant.DBConstants.*;

/**
 * 元数据表，Schema如下
 * rowkey: 数据集名称
 * meta:index_meta  -- List<IndexMeta>序列化后的值
 * meta:main_table_meta  -- 主表索引元信息，IndexMeta序列化后的值
 * meta:desc -- 数据集描述，字符串类型
 *
 * @author Haocheng Wang
 * Created on 2022/10/11
 */
public class MetaTable {

  private static Logger logger = LoggerFactory.getLogger(MetaTable.class);

  private Table dataSetMetaTable;

  public MetaTable(Table dataSetMetaTable) {
    this.dataSetMetaTable = dataSetMetaTable;
  }

  public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
    if (!dataSetExists(dataSetName)) {
      String msg = String.format("Dataset meta of [%s] does not exist", dataSetName);
      logger.error(msg);
      throw new IOException(msg);
    } else {
      Result result = dataSetMetaTable.get(new Get(dataSetName.getBytes()));
      return fromResult(result);
    }
  }

  public boolean dataSetExists(String datasetName) throws IOException {
    return dataSetMetaTable.exists(new Get(datasetName.getBytes()));
  }

  public void putDataSet(DataSetMeta dataSetMeta) throws IOException {
    dataSetMetaTable.put(getPut(dataSetMeta));
  }

  public void deleteDataSetMeta(String datasetName) throws IOException {
    dataSetMetaTable.delete(new Delete(datasetName.getBytes()));
    logger.info(String.format("Meta data of dataset [%s] has been removed from meta table", datasetName));
  }

  // TODO
  public List<DataSetMeta> listDataSetMeta() throws IOException {
    return null;
  }

  /**
   * Convert DataSetMeta object into HBase Put object for dataset creation.
   */
  private Put getPut(DataSetMeta dataSetMeta) throws IOException {
    String dataSetName = dataSetMeta.getDataSetName();
    Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
    // row key - 数据集名称
    Put p = new Put(Bytes.toBytes(dataSetName));
    List<IndexMeta> indexMetaList = new ArrayList<>();
    for (IndexType type : indexMetaMap.keySet()) {
      indexMetaList.addAll(indexMetaMap.get(type));
    }
    // meta:index_meta
    p.addColumn(Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_INDEX_META_QUALIFIER),
        SerializerUtils.serializeList(indexMetaList, IndexMeta.class));
    // meta:main_table_meta
    p.addColumn(Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_MAIN_TABLE_META_QUALIFIER),
        SerializerUtils.serializeObject(dataSetMeta.getCoreIndexMeta()));
    // meta:desc
    p.addColumn(Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_DESC_QUALIFIER),
        Bytes.toBytes(dataSetMeta.getDesc()));
    return p;
  }

  /**
   * Convert HBase Result object into DataSetMeta object.
   */
  private DataSetMeta fromResult(Result result) throws IOException {
    byte[] CF = Bytes.toBytes(META_TABLE_COLUMN_FAMILY);
    byte[] INDEX_METAS_CQ = Bytes.toBytes(META_TABLE_INDEX_META_QUALIFIER);
    byte[] MAIN_TABLE_CQ = Bytes.toBytes(META_TABLE_MAIN_TABLE_META_QUALIFIER);
    String dataSetName = new String(result.getRow());
    Cell cell1 = result.getColumnLatestCell(CF, INDEX_METAS_CQ);
    Cell cell2 = result.getColumnLatestCell(CF, MAIN_TABLE_CQ);
    List<IndexMeta> indexMetaList = SerializerUtils.deserializeList(CellUtil.cloneValue(cell1), IndexMeta.class);
    IndexMeta mainTableMeta = (IndexMeta) SerializerUtils.deserializeObject(CellUtil.cloneValue(cell2), IndexMeta.class);
    return new DataSetMeta(dataSetName, indexMetaList, mainTableMeta);
  }

  public void close() {
    try {
      dataSetMetaTable.close();
    } catch (IOException e) {
      logger.error("Failed to close meta table instance, exception log: {}", e.getMessage());
    }
  }
}
