package cn.edu.whu.trajspark.database.load.type;

import java.io.Serializable;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
public enum TextSplitType implements Serializable {
  /**
   * The csv.
   */
  CSV(","),

  /**
   * The tsv.
   */
  TSV("\t"),

  /**
   * The geojson.
   */
  GEOJSON(""),

  /**
   * The wkt.
   */
  WKT("\t"),

  /**
   * The wkb.
   */
  WKB("\t"),

  COMMA(","),

  TAB("\t");

  /**
   * Gets the file data splitter.
   *
   * @param str the str
   * @return the file data splitter
   */
  public static TextSplitType getTextSplitType(String str) {
    for (TextSplitType me : TextSplitType.values()) {
      if (me.getDelimiter().equalsIgnoreCase(str) || me.name().equalsIgnoreCase(str)) {
        return me;
      }
    }
    throw new IllegalArgumentException(
        "[" + TextSplitType.class + "] Unsupported FileDataSplitter:" + str);
  }

  /**
   * The splitter.
   */
  private final String splitter;

  /**
   * Instantiates a new file data splitter.
   *
   * @param splitter the splitter
   */
  TextSplitType(String splitter) {
    this.splitter = splitter;
  }

  /**
   * Gets the delimiter.
   *
   * @return the delimiter
   */
  public String getDelimiter() {
    return this.splitter;
  }

  }
