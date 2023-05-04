package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.base.util.SerializerUtils;
import cn.edu.whu.trajspark.index.spatial.XZ2IndexStrategy;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * @author Haocheng Wang
 * Created on 2022/10/23
 */
public class IndexMetaTest extends TestCase {

  public void testSerialize() throws IOException {
    XZ2IndexStrategy indexStrategy = new XZ2IndexStrategy();
    IndexMeta indexMeta = new IndexMeta(true, indexStrategy,
        "data_set_name", "defaule_index_name");
    byte[] bytes = SerializerUtils.serializeObject(indexMeta);
    IndexMeta indexMeta1 = (IndexMeta) SerializerUtils.deserializeObject(bytes, IndexMeta.class);
    assert indexMeta.equals(indexMeta1);
  }

}