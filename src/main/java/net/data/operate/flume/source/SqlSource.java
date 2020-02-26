package net.data.operate.flume.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;


/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-12
 * @desc
 */
public class SqlSource extends AbstractSource implements Configurable, PollableSource {
    private String url;
    private Long waitTime;

    @Override
    public Status process()   {
        try {
            SimpleEvent event = new SimpleEvent();
            Operate operate = new Operate();
            ResultSet res = operate.selectJob();
            JSONObject jobj = new JSONObject();
            while (res.next()) {
                int m = res.getMetaData().getColumnCount();
                for (int k = 1; k <= m; k++) {
                    String key = res.getMetaData().getColumnName(k);
                    jobj.put(key, res.getObject(k));
                    event.setBody((JSONObject.toJSONString(jobj)).getBytes());
                    event.setHeaders(new HashMap<>(0));
                    getChannelProcessor().processEvent(event);
                }
                Thread.sleep(waitTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        url = context.getString("url");
        waitTime = context.getLong("waitTime", 300L);
    }

    public static JSONObject crawlApi(String url) throws IOException {
        Document doc = Jsoup.connect(url).get();
        String res = doc.text();
        JSONObject jobj = (JSONObject) JSONObject.parse(res);
        return jobj;
    }
}
