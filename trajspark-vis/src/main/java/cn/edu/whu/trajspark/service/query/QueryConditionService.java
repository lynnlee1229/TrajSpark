package cn.edu.whu.trajspark.service.query;

import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition;
import cn.edu.whu.trajspark.query.condition.SpatialQueryCondition.SpatialQueryType;
import cn.edu.whu.trajspark.query.condition.SpatialTemporalQueryCondition;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.whu.trajspark.constant.DBConstants.TIME_ZONE;


public class QueryConditionService {

  public QueryConditionService() {
  }

  public SpatialQueryCondition creatIntersectSpatialCondition(String queryWindow) throws ParseException {
    WKTReader wktReader = new WKTReader();
    Geometry envelopeIntersect = wktReader.read(queryWindow);
    return new SpatialQueryCondition(envelopeIntersect,
        SpatialQueryType.INTERSECT);
  }

  public SpatialQueryCondition creatContainedSpatialCondition(String queryWindow) throws ParseException {
    WKTReader wktReader = new WKTReader();
    Geometry envelopeIntersect = wktReader.read(queryWindow);
    return new SpatialQueryCondition(envelopeIntersect,
        SpatialQueryType.CONTAIN);
  }

  public TemporalQueryCondition creatIntersectTemporalCondition(String startTime, String endTime){
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start = ZonedDateTime.parse(startTime, dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse(endTime, dateTimeFormatter);
    TimeLine timeLine = new TimeLine(start, end);
    List<TimeLine> timeLineList = new ArrayList<>();
    timeLineList.add(timeLine);
    return new TemporalQueryCondition(timeLineList,
        TemporalQueryType.INTERSECT);
  }

  public TemporalQueryCondition creatContainedTemporalCondition(String startTime, String endTime){
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start = ZonedDateTime.parse(startTime, dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse(endTime, dateTimeFormatter);
    TimeLine timeLine = new TimeLine(start, end);
    List<TimeLine> timeLineList = new ArrayList<>();
    timeLineList.add(timeLine);
    return new TemporalQueryCondition(timeLineList,
        TemporalQueryType.CONTAIN);
  }

  public SpatialTemporalQueryCondition creatContainedSTCondition(String queryWindow, String startTime, String endTime)
      throws ParseException {
    SpatialQueryCondition spatialQueryCondition = creatContainedSpatialCondition(queryWindow);
    TemporalQueryCondition temporalQueryCondition = creatContainedTemporalCondition(startTime, endTime);
    return new SpatialTemporalQueryCondition(spatialQueryCondition, temporalQueryCondition);
  }

  public SpatialTemporalQueryCondition creatIntersectSTCondition(String queryWindow, String startTime, String endTime)
      throws ParseException {
    SpatialQueryCondition spatialQueryCondition = creatIntersectSpatialCondition(queryWindow);
    TemporalQueryCondition temporalQueryCondition = creatIntersectTemporalCondition(startTime, endTime);
    return new SpatialTemporalQueryCondition(spatialQueryCondition, temporalQueryCondition);
  }

}
