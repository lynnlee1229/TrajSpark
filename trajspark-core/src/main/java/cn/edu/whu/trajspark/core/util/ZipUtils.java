package cn.edu.whu.trajspark.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Lynn Lee
 * @date 2022/9/17
 **/
public class ZipUtils implements Serializable {
  public static byte[] gZip(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Throwable var3 = null;

    byte[] gZipBytes;
    try {
      GZIPOutputStream gzip = new GZIPOutputStream(bos);
      Throwable var5 = null;

      try {
        gzip.write(data);
        gzip.finish();
        gzip.close();
        gZipBytes = bos.toByteArray();
      } catch (Throwable var28) {
        var5 = var28;
        throw var28;
      } finally {
        if (gzip != null) {
          if (var5 != null) {
            try {
              gzip.close();
            } catch (Throwable var27) {
              var5.addSuppressed(var27);
            }
          } else {
            gzip.close();
          }
        }

      }
    } catch (Throwable var30) {
      var3 = var30;
      throw var30;
    } finally {
      if (bos != null) {
        if (var3 != null) {
          try {
            bos.close();
          } catch (Throwable var26) {
            var3.addSuppressed(var26);
          }
        } else {
          bos.close();
        }
      }

    }

    return gZipBytes;
  }

  public static byte[] unGZip(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Throwable var3 = null;

    try {
      GZIPInputStream gzip = new GZIPInputStream(bis);
      Throwable var5 = null;

      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Throwable var7 = null;

        try {
          byte[] buf = new byte[1024];

          int num;
          while ((num = gzip.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, num);
          }

          byte[] unGZipBytes = baos.toByteArray();
          baos.flush();
          return unGZipBytes;
        } catch (Throwable var52) {
          var7 = var52;
          throw var52;
        } finally {
          if (baos != null) {
            if (var7 != null) {
              try {
                baos.close();
              } catch (Throwable var51) {
                var7.addSuppressed(var51);
              }
            } else {
              baos.close();
            }
          }

        }
      } catch (Throwable var54) {
        var5 = var54;
        throw var54;
      } finally {
        if (gzip != null) {
          if (var5 != null) {
            try {
              gzip.close();
            } catch (Throwable var50) {
              var5.addSuppressed(var50);
            }
          } else {
            gzip.close();
          }
        }

      }
    } catch (Throwable var56) {
      var3 = var56;
      throw var56;
    } finally {
      if (bis != null) {
        if (var3 != null) {
          try {
            bis.close();
          } catch (Throwable var49) {
            var3.addSuppressed(var49);
          }
        } else {
          bis.close();
        }
      }

    }
  }
}
