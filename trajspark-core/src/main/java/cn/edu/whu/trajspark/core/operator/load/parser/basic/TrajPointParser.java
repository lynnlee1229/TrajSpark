package cn.edu.whu.trajspark.core.operator.load.parser.basic;

import cn.edu.whu.trajspark.core.common.point.TrajPoint;
import cn.edu.whu.trajspark.core.conf.data.Mapping;
import cn.edu.whu.trajspark.core.conf.data.TrajPointConfig;
import cn.edu.whu.trajspark.core.util.DataTypeUtils;
import cn.edu.whu.trajspark.core.util.DateUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2022/9/15
 **/
public class TrajPointParser {

  public static TrajPoint parse(String rawString, TrajPointConfig config, String splitter) throws
      IOException {
    try {
      String[] record = rawString.split(splitter);
      String id = record[config.getPointId().getIndex()];
      ZonedDateTime
          time = DateUtils.parse(config.getTime().getBasicDataTypeEnum(),
          record[config.getTime().getIndex()], config.getTime());
      double lat = Double.parseDouble(record[config.getLat().getIndex()]);
      double lng = Double.parseDouble(record[config.getLng().getIndex()]);
      Map<String, Object> metas = new HashMap(config.getTrajPointMetas().size());
      Iterator var11 = config.getTrajPointMetas().iterator();

      while (var11.hasNext()) {
        Mapping m = (Mapping) var11.next();
        metas.put(m.getMappingName(),
            DataTypeUtils.parse(record[m.getIndex()], m.getDataType(), m.getSourceData()));
      }

      return new TrajPoint(id, time, lng, lat, metas);
    } catch (Exception var13) {
      throw new IOException(var13.getMessage());
    }
  }

}
