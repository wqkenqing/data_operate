package net.data.operate.test;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2021/7/7
 * @desc 访问atlas RESTful 接口
 */
public class AtlasRestTest {
    public static void main(String[] args) throws IOException {
        Document doc = Jsoup.connect("http://192.168.10.240:21000/api/atlas/v2/types/entitydef/name/_ALL_ENTITY_TYPES")
                .header("Accept", "application/json, text/javascript, */*; q=0.01")
                .header("Cookie", "ATLASSESSIONID=1l3scihx0tf4h1xjljwj06xtau")
                .ignoreContentType(true)
                .get();
        String content = doc.text();
        System.out.printf(content);
    }
}
