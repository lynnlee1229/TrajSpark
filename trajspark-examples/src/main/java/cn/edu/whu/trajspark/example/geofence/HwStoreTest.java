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
        /**
         * 1.读取配置文件
         * 提供三种方式：
         * HDFS，需要指定fs和路径（例：传入2参数localhost:9000 path）
         * 本地文件系统，需要指定路径（例：传入1参数 path）
         * 资源文件，不需传参，会从项目资源文件中读取，仅本地测试使用
         */
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
        // 本地测试时可以传入第三个参数，指定是否本地master运行
        boolean isLocal = false;
        int localIndex = 2;
        try {
            isLocal = Boolean.parseBoolean(args[localIndex]);
        } catch (Exception ignored) {
        }
        // 2.解析配置文件
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        // 3.初始化sparkSession
        LOGGER.info("Init sparkSession...");
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HwStoreTest.class.getName(), isLocal)) {
            // 4.加载轨迹数据
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
                            exampleConfig.getDataConfig());
            long start = System.currentTimeMillis();
            // 5.存储轨迹数据
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
