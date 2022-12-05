package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.coding.XZ2PCoding;
import cn.edu.whu.trajspark.coding.poscode.PosCode;
import cn.edu.whu.trajspark.core.common.mbr.MinimumBoundingBox;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.database.util.TrajectorySerdeUtils;
import cn.edu.whu.trajspark.datatypes.ByteArray;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.index.spatial.XZ2PlusIndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Haocheng Wang
 * Created on 2022/11/16
 */
public class XZ2PQueryTest {
  static String DATASET_NAME = "xz2p_intersect_query_test";
  static SpatialQueryCondition spatialIntersectQueryCondition;
  static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
          "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
          "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";
  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Envelope envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT).getEnvelopeInternal();
      Envelope envelopeContained = wktReader.read(QUERY_WKT_CONTAIN).getEnvelopeInternal();
      spatialIntersectQueryCondition = new SpatialQueryCondition(envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition = new SpatialQueryCondition(envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2PlusIndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
  }

  @Test
  public void testIndexTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2PlusIndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    System.out.println("to put " + trips.size() + "trajectories");
    for (Trajectory trajectory : trips) {
      System.out.println(list.get(0).getIndexStrategy().index(trajectory));
    }
  }

  @Test
  public void testExecuteQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialContainedQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    for (Trajectory result : results) {
      System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy().index(result));
    }
    assertEquals(19, spatialQuery.executeQuery().size());
  }

  @Test
  public void testGetAnswer() throws URISyntaxException, ParseException, IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
            new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    WKTReader wktReader = new WKTReader();
    Polygon envelope = (Polygon) wktReader.read(QUERY_WKT_CONTAIN).getEnvelope();
    System.out.println(trips.size());
    for (Trajectory trajectory : trips) {
      if (envelope.contains(trajectory.getLineString())) {
        System.out.println(dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy().index(trajectory));
        // System.out.println(trajectory);
      }
    }
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }

  @org.junit.jupiter.api.Test
  public void testSingleGetTrajectory() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    byte[] target = Bytes.fromHex("000100000001000000012156ad340a4342514244533431");
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    Result result = dataTable.get(new Get(target));
    Envelope envelope = spatialContainedQueryCondition.getQueryWindow();
    Polygon queryPolygon = new MinimumBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()).toPolygon(4326);
    Trajectory t = TrajectorySerdeUtils.getTrajectory(result);
    System.out.println("Query polygon contains: " + queryPolygon.contains(t.getLineString()));
    System.out.println("Trajectory WKT: " + t.getLineString());
    XZ2PlusIndexStrategy xz2PlusIndexStrategy = (XZ2PlusIndexStrategy) dataTable.getDataSetMeta().getIndexMetaList().get(0).getIndexStrategy();
    ByteArray spatialCode = xz2PlusIndexStrategy.extractSpatialCode(new ByteArray(target));
    XZ2PCoding xz2PCoding = (XZ2PCoding) xz2PlusIndexStrategy.getSpatialCoding();
    long xz2Code = xz2PCoding.getXZ2Code(spatialCode);
    System.out.println("Trajectory XZ2 Code: " + xz2Code);
    System.out.println("Trajectory XZ2 Polygon: " + xz2PCoding.getCodingPolygon(spatialCode));
    System.out.println("Trajectory XZ2 PosCode: " + xz2PCoding.getPosCode(spatialCode));
    System.out.println("Query WKT: " + QUERY_WKT_CONTAIN);
  }

}
