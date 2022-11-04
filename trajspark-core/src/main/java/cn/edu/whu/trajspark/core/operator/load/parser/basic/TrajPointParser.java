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
      int pidIdx = config.getPointId().getIndex();
      String id;
      if (pidIdx < 0) {
        id = null;
      } else {
        id = record[config.getPointId().getIndex()];
      }
      ZonedDateTime
          time = DateUtils.parse(config.getTime().getBasicDataTypeEnum(),
          record[config.getTime().getIndex()], config.getTime());
      double lat = Double.parseDouble(record[config.getLat().getIndex()]);
      double lng = Double.parseDouble(record[config.getLng().getIndex()]);
      Map<String, Object> metas = new HashMap(config.getTrajPointMetas().size());
      Iterator iter = config.getTrajPointMetas().iterator();

      while (iter.hasNext()) {
        Mapping m = (Mapping) iter.next();
        metas.put(m.getMappingName(),
            DataTypeUtils.parse(record[m.getIndex()], m.getDataType(), m.getSourceData()));
      }

      return new TrajPoint(id, time, lng, lat, metas);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

}
