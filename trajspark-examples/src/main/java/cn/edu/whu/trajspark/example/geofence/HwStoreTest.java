package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.core.operator.load.ILoader;
import cn.edu.whu.trajspark.core.operator.store.IStore;
import cn.edu.whu.trajspark.core.util.IOUtils;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.example.conf.ExampleConfig;
import cn.edu.whu.trajspark.example.util.FileSystemUtils;
import cn.edu.whu.trajspark.example.util.SparkSessionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class HwStoreTest {
    private static final Logger LOGGER = Logger.getLogger(HwStoreTest.class);
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
            InputStream resourceAsStream = HwStoreTest.class.getClassLoader()
                    .getResourceAsStream("ioconf/hw_store.json");
            fileStr = IOUtils.readFileToString(resourceAsStream);
        }
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init sparkSession...");
        boolean isLocal = false;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HwStoreTest.class.getName(), isLocal)) {
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
                            exampleConfig.getDataConfig());
            long start = System.currentTimeMillis();
            IStore iStore = IStore.getStore(exampleConfig.getStoreConfig());
            iStore.storeTrajectory(trajRDD);
            long end = System.currentTimeMillis();
            long cost = (end - start);
            System.out.printf("HBase store cost %dmin %ds \n",cost /60000, cost%60000/1000);
             LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet("GEOFENCE_TRAJECTORY_HW");
  }
}
