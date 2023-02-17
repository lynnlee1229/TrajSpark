package cn.edu.whu.trajspark.query;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.database.DataSet;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.ExampleTrajectoryUtil;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import cn.edu.whu.trajspark.query.basic.IDTemporalQuery;
import cn.edu.whu.trajspark.query.condition.IDQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * @author Xu Qi
 * @since 2022/11/11
 */
public class IDTemporalQueryTest {

  static String DATASET_NAME = "ID_Temporal_query_test1";
  public static TemporalQueryCondition temporalContainCondition;
  public static TemporalQueryCondition temporalIntersectCondition;
  public static IDQueryCondition idQueryCondition = new IDQueryCondition("CBQBDS");
  public static IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
  public static TimeLine testTimeLine1;
  public static TimeLine testTimeLine2;

  static List<TimeLine> timeLineList = new ArrayList<>();
  static {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(
            ZoneId.systemDefault());
    ZonedDateTime start = ZonedDateTime.parse("2015-12-25 06:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2015-12-25 07:00:00", dateTimeFormatter);
    ZonedDateTime start1 = ZonedDateTime.parse("2015-12-25 15:00:00", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2015-12-25 16:00:00", dateTimeFormatter);
    testTimeLine1 = new TimeLine(start, end);
    testTimeLine2 = new TimeLine(start1, end1);
    timeLineList.add(testTimeLine1);
    timeLineList.add(testTimeLine2);
    temporalIntersectCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
    temporalContainCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.CONTAIN);
  }

  @Test
  public void testPutTrajectory() throws IOException, URISyntaxException {
    Database instance = Database.getInstance();
    // create dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(true, IDTIndexStrategy, DATASET_NAME, "default"));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // insert data
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory t : trips) {
      indexTable.putForMainTable(t);
    }
  }

  @Test
  public void testGetIndexRanges() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, temporalContainCondition, idQueryCondition);
    List<RowKeyRange> scanRanges = IDTemporalQuery.getIndexRanges();
    System.out.println("Multi InnerBin ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : " + IDTIndexStrategy.parseIndex2String(scanRange.getStartKey()) + " end : "
              + IDTIndexStrategy.parseIndex2String(scanRange.getEndKey()) + "isContained "
              + scanRange.isContained());
    }
  }


  @Test
  void executeINTERSECTQuery() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, temporalIntersectCondition, idQueryCondition);
    List<Trajectory> trajectories = IDTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      System.out.println(trajectory);
    }
    assertEquals(10, IDTemporalQuery.executeQuery().size());
  }

  @Test
  void executeContainQuery() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, temporalContainCondition, idQueryCondition);
    List<Trajectory> trajectories = IDTemporalQuery.executeQuery();
    System.out.println(trajectories.size());
    for (Trajectory trajectory : trajectories) {
      System.out.println(trajectory);
    }
    assertEquals(6, IDTemporalQuery.executeQuery().size());
  }

  @Test
  public void testGetAnswer() throws URISyntaxException, IOException {
    List<Trajectory> trips = ExampleTrajectoryUtil.parseFileToTrips(
        new File(ExampleTrajectoryUtil.class.getResource("/CBQBDS").toURI()));
    int i = 0;
    int j = 0;
    for (Trajectory trajectory : trips) {
      ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
      TimeLine trajTimeLine = new TimeLine(startTime, endTime);
      for (TimeLine queryTimeLine : timeLineList) {
        if (queryTimeLine.contain(trajTimeLine)) {
          i++;
        }
        if (queryTimeLine.intersect(trajTimeLine)) {
          // System.out.println(new TimeLine(startTime, endTime));
          System.out.println(trajectory);
          j++;
        }
      }
    }
    System.out.println("CONTAIN: " + i);
    System.out.println("INTERSECT: " + j);
  }

  @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}