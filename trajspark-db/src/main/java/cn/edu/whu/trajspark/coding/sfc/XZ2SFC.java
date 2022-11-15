package cn.edu.whu.trajspark.coding.sfc;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.whu.trajspark.constant.CodingConstants.*;


/**
 * @author Haocheng Wang
 * Created on 2022/11/2
 */
public class XZ2SFC implements Serializable {

  static List<XElement> LevelOneElements = new XElement(0.0, 0.0, 1.0, 1.0, 1.0).children();

  static XElement LevelTerminator = new XElement(-1.0, -1.0, -1.0, -1.0, 0);

  static ConcurrentHashMap<Short, XZ2SFC> cache = new ConcurrentHashMap<>();

  static double LogPointFive = Math.log(0.5);

  public static XZ2SFC getInstance(short g) {
    XZ2SFC sfc = cache.get(g);
    if (sfc == null) {
      sfc = new XZ2SFC(g, new Bound(XZ2_X_MIN,XZ2_Y_MIN,  XZ2_X_MAX, XZ2_Y_MAX));
      cache.put(g, sfc);
    }
    return sfc;
  }

  protected short g;
  protected double xLo;
  protected double xHi;
  protected double yLo;
  protected double yHi;
  protected double xSize;
  protected double ySize;

  public XZ2SFC(short g, Bound bound) {
    this.g = g;
    xLo = bound.xmin;
    xHi = bound.xmax;
    yLo = bound.ymin;
    yHi = bound.ymax;
    xSize = xHi - xLo;
    ySize = yHi - yLo;
  }

  public long index(Bound bound) {
    return index(bound.xmin, bound.xmax, bound.ymin, bound.ymax, false);
  }

  /**
   * Index a polygon by it's bounding box
   *
   * @param xmin    min x value in xBounds
   * @param ymin    min y value in yBounds
   * @param xmax    max x value in xBounds, must be >= xmin
   * @param ymax    max y value in yBounds, must be >= ymin
   * @param lenient standardize boundaries to valid values, or raise an exception
   * @return z value for the bounding box
   */
  public long index(double xmin, double xmax, double ymin, double ymax, boolean lenient) {
    // normalize inputs to [0,1]
    Bound nBound = normalize(xmin, ymin, xmax, ymax, lenient);
    double nxmin = nBound.xmin;
    double nymin = nBound.ymin;
    double nxmax = nBound.xmax;
    double nymax = nBound.ymax;

    // calculate the length of the sequence code (section 4.1 of XZ-Ordering paper)
    double maxDim = Math.max(nxmax - nxmin, nymax - nymin);

    // l1 (el-one) is a bit confusing to read, but corresponds with the paper's definitions
    int l1 = (int) Math.floor(Math.log(maxDim) / LogPointFive);

    // the length will either be (l1) or (l1 + 1)
    int length;
    if (l1 >= g) {
      length = g;
    } else {
      double w2 = Math.pow(0.5, l1 + 1); // width of an element at resolution l2 (l1 + 1)

      if (predicate(nxmin, nxmax, w2) && predicate(nymin, nymax, w2)) {
        length = l1 + 1;
      } else {
        length = l1;
      }
    }
    return sequenceCode(nxmin, nymin, length);
  }

  // predicate for checking how many axis the polygon intersects
  // math.floor(min / w2) * w2 == start of cell containing min
  private boolean predicate(double min, double max, double w2) {
    return max <= (Math.floor(min / w2) * w2) + (2 * w2);
  }

  /**
   * Computes the sequence code for a given point - for polygons this is the lower-left corner.
   * <p>
   * Based on Definition 2 from the XZ-Ordering paper
   *
   * @param x      normalized x value [0,1]
   * @param y      normalized y value [0,1]
   * @param length length of the sequence code that will be generated
   * @return
   */
  private long sequenceCode(double x, double y, int length) {
    double xmin = 0.0;
    double ymin = 0.0;
    double xmax = 1.0;
    double ymax = 1.0;

    long cs = 0L;

    int i = 0;
    while (i < length) {
      double xCenter = (xmin + xmax) / 2.0;
      double yCenter = (ymin + ymax) / 2.0;
      boolean bool1 = x < xCenter;
      boolean bool2 = y < yCenter;
      if (bool1) {
        if (bool2) {
          cs += 1L;
          xmax = xCenter;
          ymax = yCenter;
        } else {
          cs += 1L + 2L * (Math.pow(4, g - i) - 1L) / 3L;
          xmax = xCenter;
          ymin = yCenter;
        }
      } else {
        if (bool2) {
          cs += 1L + 1L * (Math.pow(4, g - i) - 1L) / 3L;
          xmin = xCenter;
          ymax = yCenter;
        } else {
          cs += 1L + 3L * (Math.pow(4, g - i) - 1L) / 3L;
          xmin = xCenter;
          ymin = yCenter;
        }
      }
      i += 1;
    }
    return cs;
  }

  /**
   * Computes an interval of sequence codes for a given point - for polygons this is the lower-left corner.
   *
   * @param x       normalized x value [0,1]
   * @param y       normalized y value [0,1]
   * @param length  length of the sequence code that will used as the basis for this interval
   * @param partial true if the element partially intersects the query window, false if it is fully contained
   * @return
   */
  long[] sequenceInterval(double x, double y, short length, boolean partial) {
    long min = sequenceCode(x, y, length);
    // if a partial match, we just use the single sequence code as an interval
    // if a full match, we have to match all sequence codes starting with the single sequence code
    long max;
    if (partial) {
      max = min;
    } else {
      // from lemma 3 in the XZ-Ordering paper
      max = (long) (min + (Math.pow(4, g - length + 1) - 1L) / 3L);
    }
    return new long[]{min, max};
  }

  private Bound normalize(double xmin,
                          double ymin,
                          double xmax,
                          double ymax,
                          boolean lenient) {
    assert (xmin <= xmax && ymin <= ymax);

    double nxmin = 0;
    double nymin = 0;
    double nxmax = 0;
    double nymax = 0;
    try {
      if (xmin < xLo && xmax > xHi && ymin < yLo && ymax > yHi) {
        throw new IllegalArgumentException(String.format("Values out of bounds ([{} {}] [{} {}]): [{} {}] [{} {}]",
            xLo, xHi, yLo, yHi, xmin, xmax, ymin, ymax));
      }
      nxmin = (xmin - xLo) / xSize;
      nymin = (ymin - yLo) / ySize;
      nxmax = (xmax - xLo) / xSize;
      nymax = (ymax - yLo) / ySize;
    } catch (IllegalArgumentException e) {
      if (lenient) {
        double bxmin;
        if (xmin < xLo) {
          bxmin = xLo;
        } else if (xmin > xHi) {
          bxmin = xHi;
        } else {
          bxmin = xmin;
        }
        double bymin;
        if (ymin < yLo) {
          bymin = yLo;
        } else if (ymin > yHi) {
          bymin = yHi;
        } else {
          bymin = ymin;
        }
        double bxmax;
        if (xmax < xLo) {
          bxmax = xLo;
        } else if (xmax > xHi) {
          bxmax = xHi;
        } else {
          bxmax = xmax;
        }
        double bymax;
        if (ymax < yLo) {
          bymax = yLo;
        } else if (ymax > yHi) {
          bymax = yHi;
        } else {
          bymax = ymax;
        }
        nxmin = (bxmin - xLo) / xSize;
        nymin = (bymin - yLo) / ySize;
        nxmax = (bxmax - xLo) / xSize;
        nymax = (bymax - yLo) / ySize;
      }
    }
    return new Bound(nxmin, nymin, nxmax, nymax);
  }


  public List<SFCRange> ranges(double xmin, double ymin, double xmax, double ymax) {
    Bound[] query = new Bound[]{normalize(xmin, ymin, xmax, ymax, false)};
    return ranges(query, Integer.MAX_VALUE);
  }

  public List<SFCRange> ranges(Envelope envelope) {
    double xMin = envelope.getMinX();
    double yMin = envelope.getMinY();
    double xMax = envelope.getMaxX();
    double yMax = envelope.getMaxY();
    return ranges(xMin, yMin, xMax, yMax);
  }

  /**
   * Determine XZ-curve ranges that will cover a given query window
   *
   * @param query     a sequence of OR'd windows to cover, normalized to [0,1]
   * @param rangeStop a rough max value for the number of ranges to return
   * @return
   */
  private List<SFCRange> ranges(Bound[] query, int rangeStop) {
    List<SFCRange> ranges = new ArrayList<>(100);
    Deque<XElement> remaining = new ArrayDeque<>(100);
    // initial level
    remaining.addAll(LevelOneElements);
    remaining.add(LevelTerminator);

    // level of recursion
    short level = 1;

    while (level < g && !remaining.isEmpty() && ranges.size() < rangeStop) {
      XElement next = remaining.poll();
      if (next.equals(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty()) {
          level = (short) (level + 1);
          remaining.add(LevelTerminator);
        }
      } else {
        checkValue(next, level, query, ranges, remaining);
      }
    }

    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    while (!remaining.isEmpty()) {
      XElement quad = remaining.poll();
      if (quad.equals(LevelTerminator)) {
        level = (short) (level + 1);
      } else {
        long[] minMax = sequenceInterval(quad.xmin, quad.ymin, level, false);
        ranges.add(new SFCRange(minMax[0], minMax[1], false));
      }
    }

    // we've got all our ranges - now reduce them down by merging overlapping values
    // note: we don't bother reducing the ranges as in the XZ paper, as accumulo handles lots of ranges fairly well
    Collections.sort(ranges);

    SFCRange current = ranges.get(0); // note: should always be at least one range
    List<SFCRange> result = new ArrayList(ranges.size());
    int i = 1;
    while (i < ranges.size()) {
      SFCRange range = ranges.get(i);
      if (range.lower <= current.upper + 1 && range.contained == current.contained) {
        current = new SFCRange(current.lower, Math.max(current.upper, range.upper), range.contained);
      } else {
        // append the last range and set the current range for future merging
        result.add(current);
        current = range;
      }
      i += 1;
    }
    // append the last range - there will always be one left that wasn't added
    result.add(current);

    return result;
  }

  // checks if a quad is contained in the search space
  private boolean isContained(XElement quad, Bound[] query) {
    int i = 0;
    while (i < query.length) {
      if (quad.isContained(query[i])) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  // checks if a quad overlaps the search space
  private boolean isOverlapped(XElement quad, Bound[] query) {
    int i = 0;
    while (i < query.length) {
      if (quad.overlaps(query[i])) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  // checks a single value and either:
  //   eliminates it as out of bounds
  //   adds it to our results as fully matching, or
  //   adds it to our results as partial matching and queues up it's children for further processing
  void checkValue(XElement quad, Short level, Bound[] query, List<SFCRange> ranges, Deque<XElement> remaining) {
    if (isContained(quad, query)) {
      // whole range matches, happy day
      long[] minMax = sequenceInterval(quad.xmin, quad.ymin, level, false);
      ranges.add(new SFCRange(minMax[0], minMax[1], true));
    } else if (isOverlapped(quad, query)) {
      // some portion of this range is excluded
      // add the partial match and queue up each sub-range for processing
      long[] minMax = sequenceInterval(quad.xmin, quad.ymin, level, true);
      ranges.add(new SFCRange(minMax[0], minMax[1], false));
      remaining.addAll(quad.children());
    }
  }


  public Bound getXZ2Region(long codeSequence) {
    // 1. get sequence
    List<Integer> quadrantSequence = getQuadrantSequence(codeSequence);

    double xmax = 1.0;
    double xmin = 0.0;
    double ymax = 1.0;
    double ymin = 0.0;

    // 2. get grid
    for (Integer quad : quadrantSequence) {
      double xCenter = (xmax + xmin) / 2;
      double yCenter = (ymax + ymin) / 2;
      switch (quad) {
        case 0:
          xmax = xCenter;
          ymax = yCenter;
          break;
        case 1:
          xmin = xCenter;
          ymax = yCenter;
          break;
        case 2:
          xmax = xCenter;
          ymin = yCenter;
          break;
        default:
          xmin = xCenter;
          ymin = yCenter;
      }
    }

    // 3. denormalize, the polygon is enlarged.
    xmax = (xHi - xLo) * xmax + xLo;
    xmin = (xHi - xLo) * xmin + xLo;
    ymax = (yHi - yLo) * ymax + yLo;
    ymin = (yHi - yLo) * ymin + yLo;

    return new Bound(xmin, ymin, xmax, ymax);
  }

  /**
   * @param codeSequence Spatial coding value generated by this coding strategy.
   * @return Enlarged region represented by the spatial coding.
   */
  public Polygon getEnlargedRegion(long codeSequence) {
    Bound xz2Region = getXZ2Region(codeSequence);
    double xExt = xz2Region.xmin + 2 * (xz2Region.xmax - xz2Region.xmin);
    double yExt = xz2Region.ymin + 2 * (xz2Region.ymax - xz2Region.ymin);
    return new Bound(xz2Region.xmin, xz2Region.ymin, xExt, yExt).toPolygon();
  }

  /**
   * TODO: bug here
   * @param codeSequence Spatial coding value generated by this coding strategy.
   * @return Enlarged region represented by the spatial coding.
   */
  public Polygon getQuadRegion(long codeSequence, int targetQuad) {
    Bound xz2Region = getXZ2Region(codeSequence);
    double quadXMin = 0;
    double quadXMax = 0;
    double quadYMin = 0;
    double quadYMax = 0;

    double xLen = xz2Region.xmax - xz2Region.xmin;
    double yLen = xz2Region.ymax - xz2Region.ymin;

    switch (targetQuad) {
      case 0: // LB
        quadXMin = xz2Region.xmin;
        quadYMin = xz2Region.ymin;
        quadXMax = xz2Region.xmin + xLen / 2;
        quadYMax = xz2Region.ymin + yLen / 2;
        break;
      case 1: // RB
        quadXMin = xz2Region.xmin + xLen / 2;
        quadYMin = xz2Region.ymin;
        quadXMax = xz2Region.xmax;
        quadYMax = xz2Region.ymin + yLen / 2;
        break;
      case 2: // LU
        quadXMin = xz2Region.xmin;
        quadYMin = xz2Region.ymin + yLen / 2;
        quadXMax = xz2Region.xmin + xLen / 2;
        quadYMax = xz2Region.ymax;
        break;
      default:
        quadXMin = xz2Region.xmin + xLen / 2;
        quadYMin = xz2Region.ymin + yLen / 2;
        quadXMax = xz2Region.ymax;
        quadYMax = xz2Region.ymax;
    }

    // 4. get grid
    return new Bound(quadXMin, quadYMin, quadXMax, quadYMax).toPolygon();
  }


  // used for get spatial polygon only.
  // reverse sequence coding to quad sequence.
  protected List<Integer> getQuadrantSequence(long sequenceCode) {
    List<Integer> list = new ArrayList<>(g);
    for (int i = 0; i < g; i++) {
      if (sequenceCode == 0L) {
        break;
      }
      long x = ((long) Math.pow(4, g - i) - 1L)/3L;
      long y = sequenceCode / x;
      list.add((int)y);
      sequenceCode = sequenceCode - x * y - 1L;
    }
    return list;
  }


  static class Bound {
    double xmin;
    double ymin;
    double xmax;
    double ymax;

    public Bound(double xmin,double ymin, double xmax, double ymax) {
      this.xmin = xmin;
      this.ymin = ymin;
      this.xmax = xmax;
      this.ymax = ymax;
    }

    public Polygon toPolygon() {
      Coordinate[] coordinates = new Coordinate[5];
      coordinates[0] = new Coordinate(xmin, ymin);
      coordinates[1] = new Coordinate(xmin, ymax);
      coordinates[2] = new Coordinate(xmax, ymax);
      coordinates[3] = new Coordinate(xmax, ymin);
      coordinates[4] = new Coordinate(xmin, ymin);
      GeometryFactory factory = new GeometryFactory();
      return factory.createPolygon(coordinates);
    }
  }

 static class XElement {
  double xmin;
  double ymin;
  double xmax;
  double ymax;
  double length;
  double xext;
  double yext;

  public XElement(double xmin, double ymin, double xmax, double ymax, double length) {
    this.xmin = xmin;
    this.ymin = ymin;
    this.xmax = xmax;
    this.ymax = ymax;
    this.length = length;
    xext = xmax + length;
    yext = ymax + length;
  }

  public boolean isContained(Bound window) {
    return window.xmin <= xmin && window.ymin <= ymin && window.xmax >= xext && window.ymax >= yext;
  }

  public boolean overlaps(Bound window) {
    return window.xmax >= xmin && window.ymax >= ymin && window.xmin <= xext && window.ymin <= yext;
  }

  public List<XElement> children() {
    List<XElement> res = new LinkedList<>();
    double xCenter = (xmin + xmax) / 2.0;
    double yCenter = (ymin + ymax) / 2.0;
    double len = length / 2.0;
    res.add(new XElement(xmin, ymin, xCenter, yCenter, len));
    res.add(new XElement(xmin, yCenter, xCenter, ymax, len));
    res.add(new XElement(xCenter, ymin, xmax, yCenter, len));
    res.add(new XElement(xCenter, yCenter, xmax, ymax, len));
    return res;
  }

}

}
