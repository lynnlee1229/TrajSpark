package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.core.common.index.STRTreeIndex;
import cn.edu.whu.trajspark.core.common.index.TreeIndex;
import cn.edu.whu.trajspark.core.common.indexedgeom.MultiPolygonWithIndex;
import cn.edu.whu.trajspark.core.common.indexedgeom.PolygonWithIndex;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lynn Lee
 * @date 2023/3/30
 **/
public class GeofenceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceUtils.class);
  public static GeometryFactory geometryFactory = new GeometryFactory();
  public static WKTReader wktReader = new WKTReader(geometryFactory);

  public static List<Geometry> readBeijingDistricts(String path) {
    List<Geometry> polygons = new ArrayList<>(16);
    File file = new File(path);
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] items = line.split("\t");
        String id = items[0];
        Geometry polygon = wktReader.read(items[2]);
        polygon.setUserData(id);
        polygons.add(polygon);
      }
    } catch (IOException e) {
      LOGGER.error("Cannot read Beijing district file from {}", path, e);
    } catch (ParseException e) {
      LOGGER.error("Parse exception", e);
    }
    return polygons;
  }

  public static List<Geometry> readGeoFence(String path) {
    List<Geometry> polygons = new ArrayList<>(16);
    int idx = 0;
    File file = new File(path);
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (idx == 0) {
          idx++;
          continue;
        }
        String[] items = line.split(",");
        String id = line.split(",")[0];
        String wkt = line.split("\"")[1];
        Geometry polygon = wktReader.read(wkt);
        polygon.setUserData(id);
        polygons.add(polygon);
      }
    } catch (IOException e) {
      LOGGER.error("Cannot read file from {}", path, e);
    } catch (ParseException e) {
      LOGGER.error("Parse exception", e);
    }
    return polygons;
  }

  public static List<Geometry> readGeoFence(String fs, String path) throws ParseException {
    String content = FileSystemUtils.readFully(fs, path);
    List<Geometry> polygons = new ArrayList<>(16);
    int idx = 0;
    assert content != null;
    String[] lines = content.split("\n");
    for (String line : lines) {
      if (idx == 0) {
        idx++;
        continue;
      }
      try {
        String[] items = line.split(",");
        String id = line.split(",")[0];
        String wkt = line.split("\"")[1];
        Geometry polygon = wktReader.read(wkt);
        polygon.setUserData(id);
        polygons.add(polygon);
      } catch (Exception e) {
        LOGGER.error("Parse exception", e);
      }
    }
    return polygons;
  }

  public static TreeIndex<Geometry> getIndexedGeoFence(List<Geometry> geofenceList) {
    TreeIndex<Geometry> treeIndex = new STRTreeIndex<Geometry>();
    for (Geometry geometry : geofenceList) {
      if (geometry instanceof Polygon) {
        treeIndex.insert(PolygonWithIndex.fromPolygon((Polygon) geometry));
      } else if (geometry instanceof MultiPolygonWithIndex) {
        treeIndex.insert(MultiPolygonWithIndex.fromMultiPolygon((MultiPolygonWithIndex) geometry));
      } else {
        treeIndex.insert(geometry);
      }
    }
    return treeIndex;
  }

  public static TreeIndex<Geometry> getIndexedGeoFence(String fs, String path)
      throws ParseException {
    List<Geometry> geofenceList = readGeoFence(fs, path);
    return getIndexedGeoFence(geofenceList);
  }
  public static TreeIndex<Geometry> getIndexedGeoFence(String path) {
    List<Geometry> geofenceList = readGeoFence(path);
    return getIndexedGeoFence(geofenceList);
  }
  @Test
  public void test() throws ParseException {
    TreeIndex<Geometry> indexedGeoFence =
        getIndexedGeoFence("hdfs://localhost:9000", "/geofence/shenzhen_landuse.csv");
    System.out.println(indexedGeoFence.size());
  }
}