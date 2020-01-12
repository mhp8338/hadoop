package bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/11
 * @description:
 */
public class ContextCache {
    private Map<Object,Object> cache = new HashMap<>();

    public Map<Object,Object> getMap(){
        return cache;
    }

    public void writeToCache(Object key, Object value){
        cache.put(key,value);
    }

    public Object getValue(Object key){
        return cache.get(key);
    }
}
