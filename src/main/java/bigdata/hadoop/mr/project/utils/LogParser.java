package bigdata.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/27
 * @description:
 */
public class LogParser {
    public Map<String, String> parse(String log) {
        Map<String, String> info = new HashMap<>(0);
        IPParser ipParser = IPParser.getInstance();
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");
            String ip = splits[13];
            String url = splits[1];
            String country = "-";
            String city = "-";
            String province = "-";
            String time = splits[17];

            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            if (regionInfo != null) {
                country = regionInfo.getCountry();
                city = regionInfo.getCity();
                province = regionInfo.getProvince();
            }

            info.put("ip", ip);
            info.put("country", country);
            info.put("city", city);
            info.put("province", province);
            info.put("url",url);
            info.put("time",time);

        }
        return info;
    }

    public Map<String, String> parse2(String log) {
        Map<String, String> info = new HashMap<>(0);
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\t");
            String ip = splits[0];
            String country = splits[1];
            String province = splits[2];
            String city = splits[3];
            String url = splits[4];
            String time = splits[5];
            String pageId = splits[6];

            info.put("ip", ip);
            info.put("country", country);
            info.put("city", city);
            info.put("province", province);
            info.put("url",url);
            info.put("time",time);
            info.put("pageId",pageId);

        }
        return info;
    }
}
