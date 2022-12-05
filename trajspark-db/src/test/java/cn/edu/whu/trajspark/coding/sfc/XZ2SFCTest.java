package cn.edu.whu.trajspark.coding.sfc;

import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.CodingConstants.MAX_XZ2_PRECISION;
import static cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author Haocheng Wang
 * Created on 2022/11/14
 */
public class XZ2SFCTest extends TestCase {

  WKTReader wktReader = new WKTReader();
  WKTWriter wktWriter = new WKTWriter();

  static String QUERY_WKT =
          "POLYGON((114.05348463177889 22.53709431984974,114.07494230389803 22.537015043892307,114.07511396527498 22.512675173600506,114.05082388043611 22.51251659361334,114.05039472699373 22.537094319849704,114.05348463177889 22.53709431984974))";


  public void testCode() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, false);
    // Polygon p = xz2SfcWhu.getRegion(5L);
    // System.out.println(wktWriter.write(p));
    List<MultiPolygon> multiPolygons = new LinkedList<>();
    for (SFCRange range : ranges) {
      List<Polygon> polygons = new LinkedList<>();
      for (long i = range.lower; i <= range.upper; i++) {
        long code = xz2SfcWhu.index(xz2SfcWhu.getRegion(i).getEnvelopeInternal());
        if (code != i) {
          System.out.printf("true code: {%s}, but got code: {%s}. \n", i, code);
        } else {
          System.out.printf("code {%s} parsed succeed. \n", i);
        }
      }
    }
  }

  public void testSequenceCode() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, false);
    // Polygon p = xz2SfcWhu.getRegion(5L);
    // System.out.println(wktWriter.write(p));
    List<MultiPolygon> multiPolygons = new LinkedList<>();
    for (SFCRange range : ranges) {
      List<Polygon> polygons = new LinkedList<>();
      for (long i = range.lower; i <= range.upper; i++) {
        long code = xz2SfcWhu.index(xz2SfcWhu.getRegion(i).getEnvelopeInternal());
        List<Integer> quadSeq = xz2SfcWhu.getQuadrantSequence(code);
        long res = 0;
        for (int j = 0; j < quadSeq.size(); j++) {
          res += quadSeq.get(j) * ((Math.pow(4, MAX_XZ2_PRECISION - j) - 1L) / 3L) + 1L;
        }
        if (res != i) {
          System.out.printf("true code: {%s}, but got code: {%s}. \n", i, res);
        }
      }
    }
  }


  public void testRevertToSequenceList() {
    Trajectory t = getExampleTrajectory();

    List<Integer> list = XZ2SFC.getInstance(MAX_XZ2_PRECISION).getQuadrantSequence(4854295219L);
    long v2 = 0L;
    for (int i = 0; i < list.size(); i++) {
      v2 += 1L + list.get(i) *  ((long) Math.pow(4, MAX_XZ2_PRECISION - i) - 1L) / 3L;
    }
    assert 4854295219L == v2;
  }

  public void testIntersectRanges() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, false);
    for (SFCRange sfcRange : ranges) {
      System.out.println(sfcRange);
    }
    System.out.println(ranges.size());
  }

  public void testContainedRanges() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, true);
    for (SFCRange sfcRange : ranges) {
      System.out.println(sfcRange);
    }
    System.out.println(ranges.size());
  }

  public void testContainedRangesWkt() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, true);
    List<MultiPolygon> multiPolygons = new LinkedList<>();
    for (SFCRange range : ranges) {
      List<Polygon> polygons = new LinkedList<>();
      for (long i = range.lower; i <= range.upper; i++) {
        polygons.add(xz2SfcWhu.getRegion(i));
      }
      polygons.add((Polygon) wktReader.read(QUERY_WKT));
      Polygon[] polygonsArr = new Polygon[polygons.size()];
      polygons.toArray(polygonsArr);
      MultiPolygon multiPolygon = new MultiPolygon(polygonsArr, new GeometryFactory());
      multiPolygons.add(multiPolygon);
    }
    for (MultiPolygon mp : multiPolygons) {
      System.out.println(new WKTWriter().write(mp));
    }
  }

  public void testContainedElementsWkt() throws ParseException {
    Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
    cn.edu.whu.trajspark.coding.sfc.XZ2SFC xz2SfcWhu = new XZ2Coding().getXz2Sfc();
    List<SFCRange> ranges = xz2SfcWhu.ranges(envelope, true);
    List<MultiPolygon> multiPolygons = new LinkedList<>();
    List<Polygon> polygons = new LinkedList<>();
    for (SFCRange range : ranges) {
      for (long i = range.lower; i <= range.upper; i++) {
        polygons.add(xz2SfcWhu.getRegion(i));
      }
    }
    polygons.add((Polygon) wktReader.read(QUERY_WKT));
    Polygon[] polygonsArr = new Polygon[polygons.size()];
    polygons.toArray(polygonsArr);
    MultiPolygon multiPolygon = new MultiPolygon(polygonsArr, new GeometryFactory());
    multiPolygons.add(multiPolygon);
    for (MultiPolygon mp : multiPolygons) {
      System.out.println(new WKTWriter().write(mp));
    }
  }

  public void testGetEnlargedRegion() {
    WKTWriter wktWriter = new WKTWriter();
    System.out.println(wktWriter.write(XZ2SFC.getInstance(MAX_XZ2_PRECISION).getEnlargedRegion(4854295659L)));
  }

  public void testGetQuadRegion() {
  }
}