package bigdata.hadoop.hdfs.mr.project.utils;

import bigdata.hadoop.mr.project.utils.IPParser;

import org.junit.Test;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/27
 * @description:
 */
public class IPTest {
    @Test
    public void testIP() throws Exception {
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("123.116.60.97");
        System.out.println(regionInfo.getCity());
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());

    }
}
