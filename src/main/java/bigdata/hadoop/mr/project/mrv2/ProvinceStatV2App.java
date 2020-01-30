package bigdata.hadoop.mr.project.mrv2;

import bigdata.hadoop.mr.project.utils.IPParser;
import bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/27
 * @description:
 */
public class ProvinceStatV2App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ProvinceStatV2App.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path output = new Path(args[1]);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);

    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> info = logParser.parse2(log);
            context.write(new Text(info.get("province")),ONE);
        }

    }

    static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
                InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }

}
