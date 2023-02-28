package cn.edu.whu.trajspark.database.meta;

import cn.edu.whu.trajspark.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class DataSetMeta {

    private static Logger logger = LoggerFactory.getLogger(DataSetMeta.class);

    String dataSetName;
    List<IndexMeta> indexMetaList;
    IndexMeta coreIndexMeta;
    String desc = "";

    /**
     * 将默认List中的第一项为core index.
     *
     * @param dataSetName
     * @param indexMetaList
     */
    public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList) {
        this(dataSetName, indexMetaList, getCoreIndexMetaFromList(indexMetaList));
    }

    public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta) {
        checkCoreIndexMeta(indexMetaList, coreIndexMeta);
        this.dataSetName = dataSetName;
        this.indexMetaList = indexMetaList;
        this.coreIndexMeta = coreIndexMeta;
    }

    public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta, String desc) {
        this(dataSetName, indexMetaList, coreIndexMeta);
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    public IndexMeta getCoreIndexMeta() {
        return coreIndexMeta;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    public List<IndexMeta> getIndexMetaList() {
        return indexMetaList;
    }

    public Map<IndexType, List<IndexMeta>> getAvailableIndexes() {
        List<IndexMeta> indexMetaList = getIndexMetaList();
        HashMap<IndexType, List<IndexMeta>> map = new HashMap<>();
        for (IndexMeta indexMeta : indexMetaList) {
            List<IndexMeta> list = map.getOrDefault(indexMeta.getIndexType(), new LinkedList<>());
            list.add(indexMeta);
            map.put(indexMeta.getIndexType(), list);
        }
        return map;
    }

    @Override
    public String toString() {
        return "DataSetMeta{" +
                "dataSetName='" + dataSetName + '\'' +
                ", coreIndexMeta=" + coreIndexMeta +
                ", indexMetaList=" + indexMetaList +
                ", desc='" + desc + '\'' +
                '}';
    }

    private void checkCoreIndexMeta(List<IndexMeta> indexMetaList, IndexMeta coreIndexMeta) throws IllegalArgumentException {
        // 检查重复
        HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
        if (hashSet.size() != indexMetaList.size()) {
            throw new IllegalArgumentException("found duplicate index meta in the list.");
        }
        // 确认coreIndex存在
        if (coreIndexMeta == null) {
            throw new IllegalArgumentException(String.format("Index meta didn't set core index."));
        }
    }

    public void addIndexMeta(IndexMeta indexMeta) {
        checkNewIndexMeta(indexMeta);
        indexMetaList.add(indexMeta);
    }

    private void checkNewIndexMeta(IndexMeta newIndexMeta) {
        // 检查数据集名称
        if (!newIndexMeta.getDataSetName().equals(dataSetName)) {
            throw new IllegalArgumentException(String.format("Inconsistent data set name, exist: %s, new index " +
                    "meta provided: %s.", dataSetName, newIndexMeta.getDataSetName()));
        }
        // 检查索引名称重复
        for (IndexMeta indexMeta : indexMetaList) {
            if (indexMeta.getIndexTableName().equals(newIndexMeta.getIndexTableName())) {
                throw new IllegalArgumentException(String.format("Index table %s already exists.", newIndexMeta.getIndexTableName()));
            }
        }
    }

    private static IndexMeta getCoreIndexMetaFromList(List<IndexMeta> indexMetaList) {
        for (IndexMeta im : indexMetaList) {
            if (im.isMainIndex()) {
                return im;
            }
        }
        return null;
    }
    public static IndexMeta getIndexMetaByName(List<IndexMeta> indexMetaList, String tableName) {
        for (IndexMeta im : indexMetaList) {
            if (im.getIndexTableName().equals(tableName)) {
                return im;
            }
        }
        return null;
    }

    public void deleteIndex(String indexName) {
        IndexMeta target = null;
        for (IndexMeta im : indexMetaList) {
            if (im.getIndexTableName().equals(indexName)) {
                target = im;
                break;
            }
        }
        indexMetaList.remove(target);
    }
}
