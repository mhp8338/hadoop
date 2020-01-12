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
 * @description:
 */
public class HdfsWc02 {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        /*读取HDFS的文件*/
        Path input = new Path(ParamsUtils.getProperties().getProperty(Constant.INPUT_PATH));
        FileSystem fs = FileSystem.get(new URI(ParamsUtils.getProperties().getProperty(Constant.HDFS_URI)),
                new Configuration(), ParamsUtils.getProperties().getProperty(Constant.USER));

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(input, false);

        /*缓存*/
        ContextCache contextcache = new ContextCache();
        Map<Object, Object> contextMap = contextcache.getMap();


//        IMapper mapper = new WordCountMapper();
        /*反射创建*/
        Class<?> clazz = Class.forName(ParamsUtils.getProperties().getProperty(Constant.MAPPER_CLASS));
        IMapper mapper = (IMapper)clazz.newInstance();

        while (it.hasNext()) {
            LocatedFileStatus file = it.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                //word count
                mapper.count(line, contextcache);
            }

            reader.close();
            in.close();
        }


        /*输出至HDFS中*/
        Path output = new Path(ParamsUtils.getProperties().getProperty(Constant.OUTPUT_PATH));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(output, ParamsUtils.getProperties().getProperty(Constant.OUTPUT_FILE)));
        for (Map.Entry<Object, Object> entry : contextMap.entrySet()) {
            fsDataOutputStream.write((entry.getKey().toString() + " : " + entry.getValue() + "\n").getBytes());
        }
        fsDataOutputStream.close();
        fs.close();
        System.out.println("运行成功");
    }
}
