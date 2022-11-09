package cn.edu.whu.trajspark.core.common.point;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/9/7
 **/
public class TrajPoint extends BasePoint implements Serializable {
  private String pid;
  private ZonedDateTime timestamp;
  private Map<String, Object> extendedValues;

  public TrajPoint(ZonedDateTime timestamp, double lng, double lat) {
    super(lng, lat);
    this.pid = null;
    this.timestamp = timestamp;
    this.extendedValues = null;
  }

  public TrajPoint(String id, ZonedDateTime timestamp, double lng, double lat) {
    super(lng, lat);
    this.pid = id;
    this.timestamp = timestamp;
    this.extendedValues = null;
  }

  public TrajPoint(String id, ZonedDateTime timestamp, double lng, double lat,
                   Map<String, Object> extendedValues) {
    super(lng, lat);
    this.pid = id;
    this.timestamp = timestamp;
    this.extendedValues = extendedValues;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public String getPid() {
    return pid;
  }

  public ZonedDateTime getTimestamp() {
    return timestamp;
  }

  public String getTimestampString() {
    return timestamp.toLocalDateTime().toString();
  }

  public Map<String, Object> getExtendedValues() {
    return this.extendedValues == null ? null : this.extendedValues;
  }

  public void setExtendedValues(Map<String, Object> extendedValues) {
    this.extendedValues = extendedValues;
  }

  public Object getExtendedValue(String key) {
    return this.extendedValues == null ? null : this.extendedValues.get(key);
  }

  public void setExtendedValue(String key, Object value) {
    if (this.extendedValues == null) {
      this.extendedValues = new HashMap<>();
    }
    this.extendedValues.put(key, value);
  }

  public void removeExtendedValue(String key) {
    if (this.extendedValues != null) {
      this.extendedValues.remove(key);
    }
  }


  public String toString() {
    return "TrajPoint{pid='" + this.pid + '\'' + ", longitude=" + this.getLng() + ", latitude="
        + this.getLat() + ", timeStamp=" + this.getTimestamp() + ", extendedValues="
        + this.extendedValues + '}';
  }
}
