package bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/10
 * @description: 使用Java API操作HDFS文件系统
 */
public class HdfsApp {
    private static final String HDFS_PATH = "hdfs://192.168.215.100:8020";
    private FileSystem fileSystem = null;
    private Configuration configuration = null;

    /**
     * 构建一个访问指定HDFS客户端的客户端对象
     * new URI(HDFS_PATH): HDFS的URL
     * configuration: 客户端指定的参数
     * User: 客户端身份
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    /**
     * 创建文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        boolean result = fileSystem.mkdirs(new Path("/hdfsapi1/test"));
        System.out.println(result);
    }

    /**
     * 查看文件
     *
     * @throws IOException
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 创建并写入文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi1/test/mmm1.txt"));
        out.writeUTF("Hello ves:replication");
        out.flush();
        out.close();
    }

    @Test
    public void checkReplication() {
        System.out.println(configuration.get("dfs.replication"));

    }

    @Test
    public void rename() throws IOException {
        Path src = new Path("/hdfsapi1/test/mmm1.txt");
        Path dis = new Path("/hdfsapi1/test/mmm2.txt");
        System.out.println(fileSystem.rename(src, dis));
    }

    /**
     * 拷贝小文件
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocal() throws IOException {
        Path src = new Path("/Users/xuepipi/Desktop/as.txt");
        Path dst = new Path("/hdfsapi1/test/mmm2.txt");
        fileSystem.copyFromLocalFile(src, dst);

    }

    @Test
    public void copyFromLocalBigFile() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/xuepipi/Documents/Java编程思想中文版4e.pdf")));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi1/test/book.pdf"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void copyToLocal() throws IOException {
        Path src = new Path("/hdfsapi1/test/mmm2.txt");
        Path dst = new Path("/Users/xuepipi/Documents/");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/hdfsapi1/test"));
        for (FileStatus file : fileStatus) {
            short replication = file.getReplication();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            long len = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);

        }
    }

    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi1/test"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            short replication = file.getReplication();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            long len = file.getLen();
            String path = file.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void fileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/jdk-8u91-linux-x64.tar.gz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            /*文件分成多个块，每个块的名字——位置*/
            String[] names = block.getNames();
            System.out.println(Arrays.toString(names));
            for (String name : names) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength() + " : " + Arrays.toString(block.getHosts()));
            }

        }
    }

    @Test
    public void delete() throws IOException {
        boolean isDelete = fileSystem.delete(new Path("/hdfsapi1/test/dir"), true);
        System.out.println(isDelete);
    }


    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
    }


}
