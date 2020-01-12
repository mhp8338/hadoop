package bigdata.hadoop.hdfs;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/11
 * @description:
 */
public class WordCountMapper implements IMapper {
    @Override
    public void count(String line, ContextCache cache) {
        String[] words = line.split(" ");
        for(String word:words){
            cache.writeToCache(word,(Integer)cache.getMap().getOrDefault(word,0)+1);
        }

    }
}
