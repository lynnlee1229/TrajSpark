package cn.edu.whu.trajspark.util;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.operator.store.convertor.basic.GeoJsonConvertor;
import cn.edu.whu.trajspark.database.util.TrajectoryJsonUtil;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

public class GeoJsonConvertTest {
  @Test
  public void testGeojsonConvert() {
    String path = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String text = JSONUtil.readLocalTextFile(path);
    List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(text);
    JSONObject jsonObject = GeoJsonConvertor.convertGeoJson(trajectories);
    System.out.println(jsonObject);
  }

}
