package cn.edu.whu.trajspark.database.util;

import cn.edu.whu.trajspark.base.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Objects;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;

/**
 * @author Xu Qi
 * @since 2022/11/2
 */
class ParseJsonToTrajectoryTest extends TestCase {

  @Test
  public void testParseTraFeatures() {
    String path = Objects.requireNonNull(
        this.getClass().getClassLoader().getResource("traj_json/test.json")).getPath();
    String text = JSONUtil.readLocalTextFile(path);
    JSONObject feature = JSONObject.parseObject(text);
    JSONArray jsonObject = feature.getJSONArray("features");
    JSONObject object = jsonObject.getJSONObject(0);
    JSONObject properties = object.getJSONObject("properties");
    TrajFeatures trajFeatures = ParseJsonToTrajectory.parseTraFeatures(properties);
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
    Trajectory trajectory = ParseJsonToTrajectory.parseJsonToTrajectory(object.toString());
    System.out.println(trajectory);
  }
}