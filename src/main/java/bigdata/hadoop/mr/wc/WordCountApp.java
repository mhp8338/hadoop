package bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/13
 * @description:
 */
public class WordCountApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.215.100:8020");

        Job job = Job.getInstance(configuration);

        /*设置job对应的参数：主类*/
        job.setJarByClass(WordCountApp.class);

        /*设置job对应的参数：设置自定义的mapper类和reduce类*/
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        /*设置job对应的参数：设置map输出的key和value的类*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /*设置job对应的参数：设置reduce输出的key和value的类*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*设置作业的输入输出路径*/
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        /*提交job*/
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);


    }
}
