package bigdata.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/28
 * @description:
 */
public class ContentUtils {
    private static Pattern PATTERN = Pattern.compile("topicId=[0-9]+");
    public static String getPageId(String url){
        String pageId = "-";
        if(StringUtils.isBlank(url)){
            return pageId;
        }
        Matcher matcher = PATTERN.matcher(url);
        if(matcher.find()){
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }
}
