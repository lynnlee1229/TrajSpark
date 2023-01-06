package cn.edu.whu.trajspark.base.util;

import org.apache.commons.lang.NullArgumentException;

import java.util.Collection;

/**
 * @author Lynn Lee
 * @date 2022/9/8
 **/
public class CheckUtils {
  public CheckUtils() {
  }

  public static void checkEmpty(Object... o) {
    int n = o.length;

    for (int i = 0; i < n; ++i) {
      if (o[i] == null) {
        throw new NullArgumentException("The parameter can not be null!");
      }
    }

  }

  public static boolean isCollectionEmpty(Collection var0) {
    return var0 == null || var0.isEmpty();
  }
}
