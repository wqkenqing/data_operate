package net.data.operate.flume.source;


import com.alibaba.fastjson.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-18
 * @desc
 */
public class HttpSourceTest {


    public void crawlApi() throws IOException {
        Document doc = Jsoup.connect("http://192.168.10.236:9999/industry/visitors/sou").get();
        String res = doc.text();
        JSONObject jobj = (JSONObject) JSONObject.parse(res);
        System.out.println(JSONObject.toJSONString(jobj));
    }
}
