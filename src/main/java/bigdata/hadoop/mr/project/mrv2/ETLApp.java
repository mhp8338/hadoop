package bigdata.hadoop.mr.project.mrv2;

import bigdata.hadoop.mr.project.mr.PageStatApp;
import bigdata.hadoop.mr.project.utils.ContentUtils;
import bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/28
 * @description:
 */
public class ETLApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PageStatApp.class);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path output = new Path(args[1]);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }

        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> info = logParser.parse(log);
            String ip = info.get("ip");
            String country = info.get("country");
            String city = info.get("city");
            String province = info.get("province");
            String url = info.get("url");
            String time = info.get("time");
            String pageId = ContentUtils.getPageId(url);

            StringBuilder builder = new StringBuilder();
            builder.append(ip).append("\t");
            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(url).append("\t");
            builder.append(time).append("\t");
            builder.append(pageId);

            context.write(NullWritable.get(), new Text(builder.toString()));


        }
    }
}
