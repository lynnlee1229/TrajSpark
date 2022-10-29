package cn.edu.whu.trajspark.example.conf;

import cn.edu.whu.trajspark.core.conf.data.IDataConfig;
import cn.edu.whu.trajspark.core.conf.load.ILoadConfig;
import cn.edu.whu.trajspark.core.conf.process.detector.IDectorConfig;
import cn.edu.whu.trajspark.core.conf.process.noisefilter.IFilterConfig;
import cn.edu.whu.trajspark.core.conf.process.segmenter.ISegmenterConfig;
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
  private IDectorConfig dectorConfig;

  public ILoadConfig getLoadConfig() {
    return loadConfig;
  }

  public IDataConfig getDataConfig() {
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

  public IDectorConfig getDectorConfig() {
    return dectorConfig;
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
}
