package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.time.TimeIndexStrategy;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;


/**
 * @author Xu Qi
 * @since 2022/11/11
 */
class TemporalQueryTest extends TestCase {

  static String DATASET_NAME = "ID_Temporal_query_test";
  static TemporalQueryCondition temporalQueryCondition;
  static String Oid = "CBQBDS";
  static TimeIndexStrategy timeIndexStrategy = new TimeIndexStrategy(new XZTCoding());

  static {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(
            ZoneId.systemDefault());
    ZonedDateTime start = ZonedDateTime.parse("2015-12-25 00:06:45", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2015-12-25 00:36:32", dateTimeFormatter);
    ZonedDateTime start1 = ZonedDateTime.parse("2015-12-25 01:00:45", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2015-12-25 01:40:33", dateTimeFormatter);
    TimeLine timeLine = new TimeLine(start, end);
    TimeLine timeLine1 = new TimeLine(start1, end1);
    List<TimeLine> timeLineList = new ArrayList<>();
    timeLineList.add(timeLine);
    timeLineList.add(timeLine1);
    temporalQueryCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    instance.openConnection();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        timeIndexStrategy,
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

  @Test
  public void testGetIndexRanges() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    TemporalQuery temporalQuery = new TemporalQuery(dataTable, temporalQueryCondition, Oid);
    List<RowKeyRange> scanRanges = temporalQuery.getIndexRanges();
    System.out.println("Multi InnerBin ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + timeIndexStrategy.parseIndex2String(scanRange.getStartKey()) + " end : "
              + timeIndexStrategy.parseIndex2String(scanRange.getEndKey()) + "isContained "
              + scanRange.isContained());
    }
//    assert scanRanges.size() == 144;
  }


  @Test
  void executeQuery() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    TemporalQuery temporalQuery = new TemporalQuery(dataTable, temporalQueryCondition, Oid);
    List<Trajectory> trajectories = temporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
    }
    assert temporalQuery.executeQuery().size() == 2;
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}