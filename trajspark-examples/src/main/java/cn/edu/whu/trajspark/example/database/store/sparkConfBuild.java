package cn.edu.whu.trajspark.example.database.store;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class sparkConfBuild {

    public static SparkSession createSession(String className,
        boolean isLocal) {
      SparkConf sparkConf = new SparkConf();
      sparkConf.set("fs.permissions.umask-mode", "022");
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      sparkConf.set("spark.kryoserializer.buffer.max", "256m");
      sparkConf.set("spark.kryoserializer.buffer", "64m");
      if (isLocal) {
        sparkConf.setMaster("local[*]");
      }
      return SparkSession
          .builder()
          .appName(className + "_" + System.currentTimeMillis())
          .config(sparkConf)
          .getOrCreate();
    }
}
