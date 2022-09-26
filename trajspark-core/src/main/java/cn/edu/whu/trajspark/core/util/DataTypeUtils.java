package cn.edu.whu.trajspark.core.util;

import cn.edu.whu.trajspark.core.common.field.Field;
import cn.edu.whu.trajspark.core.enums.BasicDataTypeEnum;

/**
 * @author Lynn Lee
 * @date 2022/9/17
 **/
public class DataTypeUtils {
  public static Object parse(String tarStr, BasicDataTypeEnum dataType) {
    return parse(tarStr, dataType, null);
  }

  public static Object parse(String rawValue, BasicDataTypeEnum dataType, Field field) {
    switch (dataType) {
      case STRING:
        return rawValue;
      case INT:
        return Integer.parseInt(rawValue);
      case LONG:
        return Long.parseLong(rawValue);
      case DATE:
      case TIMESTAMP:
        return DateUtils.parse(dataType, rawValue, field);
      case DOUBLE:
        return Double.parseDouble(rawValue);
      default:
        throw new UnsupportedOperationException();
    }
  }

}
