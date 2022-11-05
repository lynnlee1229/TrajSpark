package cn.edu.whu.trajspark.database.util;

import static cn.edu.whu.trajspark.database.util.ParseJsonToTrajectory.parseJsonToTrajectory;
import static cn.edu.whu.trajspark.database.util.ParseJsonToTrajectory.parseTraFeatures;
import static org.junit.jupiter.api.Assertions.*;

import cn.edu.whu.trajspark.coding.utils.JSONUtil;
import cn.edu.whu.trajspark.core.common.trajectory.TrajFeatures;
import cn.edu.whu.trajspark.core.common.trajectory.Trajectory;
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
    TrajFeatures trajFeatures = parseTraFeatures(properties);
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
    Trajectory trajectory = parseJsonToTrajectory(object.toString());
    System.out.println(trajectory);
  }
}