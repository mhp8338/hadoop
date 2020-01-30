package bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/20
 * @description:
 */
public class AccessPartitioner extends Partitioner<Text,Access> {
    /**
     * @param text 手机号
     * @param access 信息
     * @param numPartitions 分区数，reduce结果
     * @return 分区
     */
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if(text.toString().startsWith("13")){
            return 0;
        }else if(text.toString().startsWith("15")){
            return 1;
        }else {
            return 2;
        }
    }
}
