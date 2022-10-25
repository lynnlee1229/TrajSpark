package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.coding.XZ2Coding;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Haocheng Wang
 * Created on 2022/10/23
 */
public class IndexMetaTest extends TestCase {

  public void testSerialize() throws IOException {
    XZ2IndexStrategy indexStrategy = new XZ2IndexStrategy();
    IndexMeta indexMeta = new IndexMeta(true, indexStrategy, "data_set_name");
    byte[] bytes = IndexMeta.serialize(indexMeta);
    IndexMeta indexMeta1 = IndexMeta.deserialize(bytes);
    assert indexMeta.equals(indexMeta1);
  }

}