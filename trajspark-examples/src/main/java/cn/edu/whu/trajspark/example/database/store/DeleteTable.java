package cn.edu.whu.trajspark.example.database.store;

import cn.edu.whu.trajspark.database.Database;
import java.io.IOException;

public class DeleteTable {

  public static void main(String[] args) throws IOException {
    Database instance = Database.getInstance();
    instance.openConnection();
    instance.deleteDataSet("DataStore_100millon_1");
  }

}
