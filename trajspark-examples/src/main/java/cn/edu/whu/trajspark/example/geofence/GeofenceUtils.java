package cn.edu.whu.trajspark.example.geofence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
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
}
