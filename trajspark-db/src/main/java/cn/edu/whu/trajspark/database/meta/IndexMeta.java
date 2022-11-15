package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.core.util.SerializerUtils;
import cn.edu.whu.trajspark.index.IndexStrategy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * @author Haocheng Wang
 * Created on 2022/9/28
 */
public class IndexMeta implements Serializable {

  boolean isMainIndex;

  IndexStrategy indexStrategy;

  String dataSetName;

  public IndexMeta() {
  }

  public IndexMeta(boolean isMainIndex, IndexStrategy indexStrategy, String dataSetName) {
    this.isMainIndex = isMainIndex;
    this.indexStrategy = indexStrategy;
    this.dataSetName = dataSetName;
  }

  public boolean isMainIndex() {
    return isMainIndex;
  }

  public IndexStrategy getIndexStrategy() {
    return indexStrategy;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public static byte[] serialize(IndexMeta indexMeta) throws IOException {
    return SerializerUtils.serializeObject(indexMeta);
  }

  public static IndexMeta deserialize(byte[] bytes) {
    IndexMeta indexMeta = (IndexMeta) SerializerUtils.deserializeObject(bytes, IndexMeta.class);
    return indexMeta;
  }

  @Override
  public String toString() {
    return "IndexMeta{" +
        "isMainIndex=" + isMainIndex +
        ", indexStrategy=" + indexStrategy +
        ", dataSetName='" + dataSetName + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexMeta indexMeta = (IndexMeta) o;
    return isMainIndex == indexMeta.isMainIndex && Objects.equals(indexStrategy, indexMeta.indexStrategy) && Objects.equals(dataSetName, indexMeta.dataSetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isMainIndex, indexStrategy, dataSetName);
  }
}
