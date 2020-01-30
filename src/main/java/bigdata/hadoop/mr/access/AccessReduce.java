package bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/20
 * @description:
 */
public class AccessReduce extends Reducer<Text, Access, NullWritable,Access> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        for (Access value : values) {
            ups+=value.getUp();
            downs+=value.getDown();
        }
        context.write(NullWritable.get(),new Access(key.toString(),ups,downs));
    }
}
