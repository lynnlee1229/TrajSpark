package cn.edu.whu.trajspark.example.database.xz2;

import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
import cn.edu.whu.trajspark.database.Database;
import cn.edu.whu.trajspark.database.meta.DataSetMeta;
import cn.edu.whu.trajspark.database.meta.IndexMeta;
import cn.edu.whu.trajspark.database.table.DataTable;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.whu.trajspark.example.database.ExampleDataUtils.parseFileToTrips;

/**
 * @author Haocheng Wang
 * Created on 2022/11/1
 */
public class DataSetManage {

  static String DATASET_NAME = "xz2_example_dataset";

  public static void insertData() throws IOException {
    // 1. get database instance
    Database instance = Database.getInstance();
    instance.openConnection();
    // 2. create xz2 dataset
    List<IndexMeta> list = new LinkedList<>();
    list.add(new IndexMeta(
        true,
        new XZ2IndexStrategy(),
        DATASET_NAME
    ));
    DataSetMeta dataSetMeta = new DataSetMeta(DATASET_NAME, list);
    instance.createDataSet(dataSetMeta);
    // 3. insert data
    List<Trajectory> trajectories =  parseFileToTrips();
    DataTable dataTable = instance.getDataTable(DATASET_NAME);
    for (Trajectory t : trajectories) {
      dataTable.put(t);
    }
  }

  public static void deleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet(DATASET_NAME);
  }
}
