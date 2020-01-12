package bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/11
 * @description: 使用HDFS API完成wordcount统计
 * 需求：统计HDFS上文件的wordcount，并将结果输出回给HDFS中
 * 功能拆解：
 * 1）读取HDFS文件
 * 2）业务处理（词频统计）
 * 3）将结果缓存
 * 4）输出回HDFS
 */
public class HdfsWc01 {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        /*读取HDFS的文件*/
        Path input = new Path("/hdfsapi1/test");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.215.100:8020"), new Configuration(), "hadoop");

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(input, false);

        /*缓存*/
        ContextCache contextcache = new ContextCache();
        Map<Object, Object> contextMap = contextcache.getMap();
        IMapper mapper = new WordCountMapper();


        while (it.hasNext()) {
            LocatedFileStatus file = it.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                //word count
                mapper.count(line,contextcache);
            }

            reader.close();
            in.close();
        }


        /*输出至HDFS中*/
        Path output = new Path("/hdfsapi1/test");
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(output, new Path("wc.out")));
        for (Map.Entry<Object, Object> entry : contextMap.entrySet()) {
            fsDataOutputStream.write((entry.getKey().toString()+" : "+entry.getValue()+"\n").getBytes());
        }
        fsDataOutputStream.close();
        fs.close();
        System.out.println("运行成功");
    }
}
