package cn.edu.whu.trajspark.database.load;

import cn.edu.whu.trajspark.base.trajectory.Trajectory;
import org.locationtech.jts.io.ParseException;

/**
 * @author Haocheng Wang
 * Created on 2023/2/19
 */
public abstract class TextTrajParser {

  public abstract Trajectory parse(String line) throws ParseException;
}
