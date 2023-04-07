package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.preprocess.HBaseStoreExample;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import com.fasterxml.jackson.core.JsonParseException;
import java.util.Objects;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Xu Qi
 * @since 2023/1/11
 */
public class HBaseLoad {
    private static final Logger LOGGER = Logger.getLogger(HBaseLoad.class);

    public static void main(String[] args) throws JsonParseException {
        String inPath = Objects.requireNonNull(
                HBaseStoreExample.class.getResource("/ioconf/geofenceAppLoadConfig.json")).getPath();
        String fileStr = JSONUtil.readLocalTextFile(inPath);
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init loading from HBase Session...");
        boolean isLocal = true;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HBaseLoad.class.getName(), isLocal)) {
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
