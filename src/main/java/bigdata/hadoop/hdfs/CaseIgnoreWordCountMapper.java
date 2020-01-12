package bigdata.hadoop.hdfs;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/11
 * @description:
 */
public class CaseIgnoreWordCountMapper implements IMapper {
    @Override
    public void count(String line, ContextCache cache) {

        String[] words = line.toLowerCase().split(" ");
        for(String word:words){
            cache.getMap().put(word,(int)(cache.getMap().getOrDefault(word,0))+1);
        }

    }
}
