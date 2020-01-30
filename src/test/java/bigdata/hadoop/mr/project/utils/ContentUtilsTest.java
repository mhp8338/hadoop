package bigdata.hadoop.mr.project.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @tag: hadoop-v1
 * @author: mhp
 * @createDate: 2020/1/28
 * @description:
 */
public class ContentUtilsTest {
    private ContentUtils contentUtils;
    @Before
    public void setUp(){
        contentUtils = new ContentUtils();
    }

    @Test
    public void test(){
        String url = "20962664424\u0001http://www.1mall.com/cms/view.do?topicId=17643\u0001http://my.1mall" +
                ".com/order/finishOrder.do?orderCode=130721X2RGK0\u0001\u00013" +
                "\u0001E52UHSN9H8UWST11TY16HUSMFT2C3BXPPCV3\u0001\u0001\u0001\u0001" +
                "\u0001PPV1RNMBPQQHQ4WGEWUQSC5E2E8N19DQ\u0001\u0001\\N\u0001113.2.131" +
                ".60\u0001\u0001msessionid:QYTHRRQMVJG8FXCY1ZT27RCEZDHM2Z44,uname:9643jiang\u0001\u00012013-07-21 " +
                "14:06:57\u0001139879628\u0001http://shop.etao.com/r-8675012967596579309-s1.htm?spm=1002.8.0.0" +
                ".R4uBJ2&patter=1&tti=esearch_Y1aBi0\u000139\u0001\u0001\\N\u000111\u0001100\u0001\u0001\u0001\u0001" +
                "\u0001Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)" +
                "\u0001Win32\u0001\u0001\u0001\u0001\u0001\u0001黑龙江省\u000111\u0001\u0001\u0001齐齐哈尔市\u0001\u000144" +
                "\u0001\u0001\u0001\u0001\u0001\\N\u0001\\N\u0001\\N\u0001\\N\u00012013-07-21";
        String pageId = ContentUtils.getPageId(url);
        System.out.println(pageId);
    }

    @After
    public void tearDown(){
        contentUtils = null;
    }

}