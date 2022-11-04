package cn.edu.whu.trajspark.core.conf.store;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({
    @JsonSubTypes.Type(
        value = HDFSStoreConfig.class,
        name = "hdfs"
    ),
    @JsonSubTypes.Type(
        value = StandaloneStoreConfig.class,
        name = "standalone"
    )
})
//, @JsonSubTypes.Type(
//    value = FileOutputConfig.class,
//    name = "file"
//), @JsonSubTypes.Type(
//    value = GeoMesaOutputConfig.class,
//    name = "geoMesa"
//)
public interface IStoreConfig extends Serializable {
  StoreType getStoreType();
}
