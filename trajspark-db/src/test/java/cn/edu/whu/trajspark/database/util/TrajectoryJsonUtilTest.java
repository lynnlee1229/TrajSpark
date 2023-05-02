package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.base.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;

import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
class TrajectoryJsonUtilTest extends TestCase {

  @Test
  public void testParseTraFeatures() {
    String path = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String text = JSONUtil.readLocalTextFile(path);
    JSONObject feature = JSONObject.parseObject(text);
    JSONArray jsonObject = feature.getJSONArray("features");
    JSONObject object = jsonObject.getJSONObject(0);
    JSONObject properties = object.getJSONObject("properties");
    JSONObject features= object.getJSONObject("trajectoryFeatures");
    TrajFeatures trajFeatures = TrajectoryJsonUtil.parseTraFeatures(features, properties);
    System.out.println(trajFeatures);
  }
  @Test
  public void testParseTrajectory() {
    String path = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String text = JSONUtil.readLocalTextFile(path);
    JSONObject feature = JSONObject.parseObject(text);
    JSONArray jsonObject = feature.getJSONArray("features");
    JSONObject object = jsonObject.getJSONObject(0);
    Trajectory trajectory = TrajectoryJsonUtil.parseJsonToTrajectory(object.toString());
    System.out.println(trajectory);
  }
  @Test
  public void testParseTrajectoryList() {
    String path = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String text = JSONUtil.readLocalTextFile(path);
    List<Trajectory> trajectories = TrajectoryJsonUtil.parseGeoJsonToTrajectoryList(text);
    System.out.println(trajectories);
  }
}