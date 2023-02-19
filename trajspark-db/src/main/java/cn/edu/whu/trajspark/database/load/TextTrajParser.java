package cn.edu.whu.trajspark.database.load;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import org.locationtech.jts.io.ParseException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/19
 */
public interface TextTrajParser {

  public Trajectory parse(String line) throws ParseException;
}
