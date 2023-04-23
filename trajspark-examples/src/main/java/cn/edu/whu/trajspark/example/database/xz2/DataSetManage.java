package cn.edu.whu.trajspark.example.database.xz2;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.IndexTable;
import cn.edu.whu.trajspark.example.database.ExampleDataUtils;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/11/1
 */
public class DataSetManage {

  static String DATASET_NAME = "xz2_example_dataset";

  public static void insertData() throws IOException {
    // 1. get database instance
    Database instance = Database.getInstance();
    // 2. create xz2 dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2IndexStrategy(),
        DATASET_NAME,
        "default"
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // 3. insert data
    List<Trajectory> trajectories =  ExampleDataUtils.parseFileToTrips();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    for (Trajectory t : trajectories) {
      indexTable.put(t, null);
    }
  }

  public static void deleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}
