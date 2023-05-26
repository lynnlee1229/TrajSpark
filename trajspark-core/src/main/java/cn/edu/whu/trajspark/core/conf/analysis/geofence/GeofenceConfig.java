package cn.edu.whu.trajspark.core.conf.analysis.geofence;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * @author Lynn Lee
 * @date 2023/5/25
 **/
public class GeofenceConfig implements Serializable {
  private String defaultFs;
  private String geofencePath;
  private boolean indexFence;
@JsonCreator
  public GeofenceConfig(
    @JsonProperty("defaultFs")String defaultFs,
    @JsonProperty("geofencePath") String geofencePath,
    @JsonProperty("indexFence")boolean indexFence) {
    this.defaultFs = defaultFs;
    this.geofencePath = geofencePath;
    this.indexFence = indexFence;
  }

  public String getDefaultFs() {
    return defaultFs;
  }

  public void setDefaultFs(String defaultFs) {
    this.defaultFs = defaultFs;
  }

  public String getGeofencePath() {
    return geofencePath;
  }

  public void setGeofencePath(String geofencePath) {
    this.geofencePath = geofencePath;
  }

  public boolean isIndexFence() {
    return indexFence;
  }

  public void setIndexFence(boolean indexFence) {
    this.indexFence = indexFence;
  }
}
