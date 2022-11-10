package cn.edu.whu.trajspark.core.conf.data;

import cn.edu.whu.trajspark.core.common.field.Field;
import cn.edu.whu.trajspark.core.enums.BasicDataTypeEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/17
 **/

public class Mapping implements Serializable {
  private Field sourceData;
  private String mappingName;
  private boolean indexable;

  /**
   * 映射类，记录原始数据映射关系
   *
   * @param sourceData  数据
   * @param mappingName 映射名（列名）
   * @param indexable   是否可索引，查询用
   */
  @JsonCreator
  public Mapping(@JsonProperty("sourceData") Field sourceData,
                 @JsonProperty("mappingName") String mappingName,
                 @JsonProperty("indexable") @JsonInclude(JsonInclude.Include.NON_NULL)
                 boolean indexable) {
    this.sourceData = sourceData;
    this.mappingName = mappingName;
    this.indexable = indexable;
  }

  public String getSourceName() {
    return this.sourceData.getSourceName();
  }

  public String getMappingName() {
    return this.mappingName;
  }

  public BasicDataTypeEnum getDataType() {
    return this.sourceData.getBasicDataTypeEnum();
  }

  public int getIndex() {
    return this.sourceData.getIndex();
  }

  public Field getSourceData() {
    return this.sourceData;
  }

  public boolean isIndexable() {
    return this.indexable;
  }

  public String toString() {
    return this.indexable
        ? this.getMappingName() + ":" + this.getDataType().getType() + ":index=true"
        : this.getMappingName() + ":" + this.getDataType().getType();
  }
}
