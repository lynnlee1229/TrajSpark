package cn.edu.whu.trajspark.example.preprocess;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.XZTCoding;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.conf.load.HBaseLoadConfig;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.datatypes.TemporalQueryType;
import cn.edu.whu.trajspark.datatypes.TimeLine;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import cn.edu.whu.trajspark.index.RowKeyRange;
import cn.edu.whu.trajspark.index.time.IDTIndexStrategy;
import cn.edu.whu.trajspark.query.condition.TemporalQueryCondition;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HBaseLoadScanExample {
    private static final Logger LOGGER = Logger.getLogger(HBaseLoadScanExample.class);

    public static byte[][] init() {
        String Oid = "010";
        IDTIndexStrategy timeIndexStrategy = new IDTIndexStrategy(new XZTCoding());
        List<TimeLine> timeLineList = new ArrayList<>();
        ZonedDateTime start = ZonedDateTime.parse("2008-11-06T22:55+08:00");
        ZonedDateTime end = ZonedDateTime.parse("2008-11-08T11:09:21+08:00");
        TimeLine timeLine = new TimeLine(start, end);
        timeLineList.add(timeLine);
        TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
        List<RowKeyRange> scanRanges = timeIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
        int size = scanRanges.size();
        return new byte[][]{scanRanges.get(0).getStartKey().getBytes(), scanRanges.get(size - 1).getEndKey().getBytes()};
    }


    public static void main(String[] args) throws JsonParseException {
        String inPath = Objects.requireNonNull(
                HBaseStoreExample.class.getResource("/ioconf/testLoadConfig.json")).getPath();
        String fileStr = JSONUtil.readLocalTextFile(inPath);
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init loading from HBase Session...");
        boolean isLocal = true;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HBaseLoadExample.class.getName(), isLocal)) {
            byte[][] init = init();
            HBaseLoadConfig hbaseLoadConfig = (HBaseLoadConfig) exampleConfig.getLoadConfig();
            hbaseLoadConfig.setSCAN_ROW_START(init[0]);
            hbaseLoadConfig.setSCAN_ROW_STOP(init[1]);
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());
            LOGGER.info("Successfully load data from HBase");
            trajRDD.collect().forEach(System.out::println);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
