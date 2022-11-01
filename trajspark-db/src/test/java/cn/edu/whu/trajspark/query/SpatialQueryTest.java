package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.datatypes.RowKeyRange;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import junit.framework.TestCase;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/27
 */
public class SpatialQueryTest extends TestCase {

  static String DATASET_NAME = "xz2_intersect_query_test";
  static SpatialQueryCondition spatialQueryCondition;
  static String QUERY_WKT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Envelope envelope = wktReader.read(QUERY_WKT).getEnvelopeInternal();
      spatialQueryCondition = new SpatialQueryCondition(envelope, SpatialQueryCondition.SpatialQueryType.INTERSECT);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2IndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : trips) {
      dataTable.put(t);
    }
  }

  public void testGetIndexRanges() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialQueryCondition);
    List<RowKeyRange> list = spatialQuery.getIndexRanges();
    assert list.size() == 144;
  }

  public void testExecuteQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialQueryCondition);
    assert spatialQuery.executeQuery().size() == 13;
  }

  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }

  // private void allllScanTrajectories() {
  //   Database instance = Database.getInstance();
  //   try {
  //     instance.openConnection();
  //     DataTable dataTable = instance.getDataTable(DATASET_NAME);
  //     SpatialQuery spatialQuery = new SpatialQuery(dataTable, spatialQueryCondition);
  //     List<RowKeyRange> list = spatialQuery.getIndexRanges();
  //     BasicSTQueryCoprocessor.QueryRequest queryRequest = spatialQuery.getQueryRequest(list);
  //     List<BasicSTQueryCoprocessor.Range> rangeList = queryRequest.getRangeList();
  //     for (BasicSTQueryCoprocessor.Range range : rangeList) {
  //       RowKeyRange r = new RowKeyRange(new ByteArray(range.getStart().toByteArray()), new ByteArray(range.getEnd().toByteArray()));
  //       List<Result> resultList = dataTable.scan(r);
  //       if (resultList.size() > 0) {
  //         System.out.println("Scan range: " + r);
  //       }
  //       for (Result result : resultList) {
  //         System.out.println(new ByteArray(result.getRow()));
  //       }
  //     }
  //   } catch (IOException e) {
  //     e.printStackTrace();
  //   }
  // }

  // public void testSingleGetTrajectory() throws IOException {
  //   Database instance = Database.getInstance();
  //   instance.openConnection();
  //   byte[] target = Bytes.fromHex("000100000000000000012156ad0943425142445331363637323232363439303339");
  //   DataTable dataTable = instance.getDataTable(DATASET_NAME);
  //   Result result = dataTable.get(new Get(target));
  //   Envelope envelope = spatialQueryCondition.getQueryWindow();;
  //   Polygon queryPolygon = new MinimumBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()).toPolygon(4326);
  //   Trajectory t = TrajectorySerdeUtils.getTrajectory(result);
  //   System.out.println(t.getLineString());
  // }
}