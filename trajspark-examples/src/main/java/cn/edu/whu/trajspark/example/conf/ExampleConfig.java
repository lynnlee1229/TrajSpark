package cn.edu.whu.trajspark.example.conf;

import cn.edu.whu.trajspark.base.util.BasicDateUtils;
import cn.edu.whu.trajspark.core.conf.analysis.geofence.GeofenceConfig;
import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajPointConfig;
import cn.edu.whu.trajspark.core.conf.data.TrajectoryConfig;
import cn.edu.whu.trajspark.core.conf.load.ILoadConfig;
import cn.edu.whu.trajspark.core.conf.process.detector.IDetectorConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.IFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.ISegmenterConfig;
import cn.edu.whu.trajspark.core.conf.process.simplifier.ISimplifierConfig;
import cn.edu.whu.trajspark.core.conf.store.IStoreConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URL;

/**
 * @author Lynn Lee
 * @date 2022/9/18
 **/
public class ExampleConfig {
  /**
   * ObjectMapper
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();
  /**
   * 数据加载配置
   */
  @JsonProperty
  private ILoadConfig loadConfig;
  /**
   * 数据映射
   */
  @JsonProperty
  private IDataConfig dataConfig;
  /**
   * 输出配置
   */
  @JsonProperty
  private IStoreConfig storeConfig;
  /**
   * 去噪参数
   */
  @JsonProperty
  private IFilterConfig filterConfig;
  /**
   * 分段参数
   */
  @JsonProperty
  private ISegmenterConfig segmenterConfig;

  /**
   * 停留识别参数
   */
  @JsonProperty
  private IDetectorConfig detectorConfig;
  @JsonProperty
  private ISimplifierConfig simplifierConfig;

  @JsonProperty
  private GeofenceConfig geofenceConfig;

  private static Boolean dateUtilInited = false;

  public GeofenceConfig getGeofenceConfig() {
    return geofenceConfig;
  }

  public ISimplifierConfig getSimplifierConfig() {
    return simplifierConfig;
  }

  public ILoadConfig getLoadConfig() {
    return loadConfig;
  }

  public IDataConfig getDataConfig() {
    if (!dateUtilInited) {
      initDateUtil(dataConfig);
      dateUtilInited = true;
    }
    return dataConfig;
  }

  public IStoreConfig getStoreConfig() {
    return storeConfig;
  }

  public IFilterConfig getFilterConfig() {
    return filterConfig;
  }

  public ISegmenterConfig getSegmenterConfig() {
    return segmenterConfig;
  }

  public IDetectorConfig getDetectorConfig() {
    return detectorConfig;
  }

  /**
   * 数据同步
   *
   * @param raw 原始字符串
   * @return : cn.edu.whu.trajspark.example.conf.ExampleConfig
   **/
  public static ExampleConfig parse(String raw) throws JsonParseException {
    try {
      return MAPPER.readValue(raw, ExampleConfig.class);
    } catch (JsonMappingException e) {
      throw new IllegalArgumentException(e.toString());
    } catch (JsonParseException e) {
      throw e;
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  /**
   * 数据同步
   *
   * @param url url
   * @return : 配置
   **/
  public static ExampleConfig parse(URL url) throws IOException {
    return MAPPER.readValue(url, ExampleConfig.class);
  }

  static void initDateUtil(IDataConfig dataConfig) {
    if (dataConfig instanceof TrajectoryConfig) {
      TrajectoryConfig trajConfig = (TrajectoryConfig) dataConfig;
      TrajPointConfig trajPointConfig = trajConfig.getTrajPointConfig();
      String zoneId = trajPointConfig.getTime().getZoneId();
      String format = trajPointConfig.getTime().getFormat();
      BasicDateUtils.updateStaticProperties(format, zoneId);
    }
  }

}
