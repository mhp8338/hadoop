package bigdata.hadoop.mr.project.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/27
 * @description:
 */
public class PVStatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PVStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("output/v1/pvstat"));

        job.waitForCompletion(true);


    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text KEY = new Text("key");
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY, ONE);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(NullWritable.get(), new LongWritable(count));
        }
    }
}
