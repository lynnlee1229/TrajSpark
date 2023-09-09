package cn.edu.whu.trajspark.example.geofence;

import cn.edu.whu.trajspark.database.Database;

import java.io.IOException;

public class HwStoreDelete {
    public static void main(String[] args) throws IOException {
        String fs = args[0];
        Database instance = Database.getInstance();
        instance.openConnection();
        instance.deleteDataSet(fs);
    }
}
