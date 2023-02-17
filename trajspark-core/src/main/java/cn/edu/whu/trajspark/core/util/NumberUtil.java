package cn.edu.whu.trajspark.core.util;

/**
 * @author Lynn Lee
 * @date 2023/2/12
 **/
public class NumberUtil {
  public NumberUtil() {
  }

  public static byte[] intToBytes(int var0, int var1) {
    byte[] var2 = new byte[var1];

    for (int var3 = 0; var3 < var1; ++var3) {
      var2[var3] = (byte) (var0 >> var3 * 8);
    }

    return var2;
  }

  public static int bytesToInt(byte[] var0, int var1) {
    if (var0 == null) {
      return 0;
    } else {
      int var2 = 0;

      for (int var3 = 0; var3 < var1; ++var3) {
        int var4 = var0[var3] & 255;
        var2 |= var4 << var3 * 8;
      }

      return var2;
    }
  }

  public static double min(double... var0) {
    double var1 = Double.MAX_VALUE;
    int var3 = (var0 = var0).length;

    for (int var4 = 0; var4 < var3; ++var4) {
      double var6 = var0[var4];
      if (var1 > var6) {
        var1 = var6;
      }
    }

    return var1;
  }

  public static double max(double... var0) {
    double var1 = Double.MIN_VALUE;
    int var3 = (var0 = var0).length;

    for (int var4 = 0; var4 < var3; ++var4) {
      double var6 = var0[var4];
      if (var1 < var6) {
        var1 = var6;
      }
    }

    return var1;
  }
}
