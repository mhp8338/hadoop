package bigdata.hadoop.hdfs;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/11
 * @description:
 */
public interface IMapper {
    /**
     * 用于词频统计
     * @param line  读进来的每一行
     * @param cache 统计词频写入缓存中;
     */
    public void count(String line, ContextCache cache);
}
