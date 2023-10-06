package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;

import java.io.IOException;
import java.io.InputStream;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author Xu Qi
 * @since 2023/1/11
 */
public class HwHBaseLoad {
    private static final Logger LOGGER = Logger.getLogger(HwHBaseLoad.class);

    public static void main(String[] args) throws IOException {
        String fileStr;
        if (args.length > 1) {
            String fs = args[0];
            String filePath = args[1];
            fileStr = FileSystemUtils.readFully(fs, filePath);
        } else if (args.length == 1) {
            String confPath = args[0];
            fileStr = IOUtils.readFileToString(confPath);
        } else {
            InputStream resourceAsStream = GeofenceFromFS.class.getClassLoader()
                    .getResourceAsStream("ioconf/hw_store.json");
            fileStr = IOUtils.readFileToString(resourceAsStream);
        }
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init loading from HBase Session...");
        boolean isLocal = false;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HwHBaseLoad.class.getName(), isLocal)) {
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
