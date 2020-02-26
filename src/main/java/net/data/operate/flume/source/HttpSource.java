package net.data.operate.flume.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.HashMap;


/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-12
 * @desc
 */
public class HttpSource extends AbstractSource implements Configurable, PollableSource {
    private String url;
    private Long waitTime;
    @Override
    public Status process() throws EventDeliveryException {
        try {
            int times = 10000;
            for (int i = 0; i < times; i++) {
                SimpleEvent event = new SimpleEvent();

                JSONObject jobj = crawlApi(url);
                event.setBody((JSONObject.toJSONString(jobj)).getBytes());
                event.setHeaders(new HashMap<>(0));
                getChannelProcessor().processEvent(event);
                Thread.sleep(waitTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
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
        return (JSONObject) JSONObject.parse(res);
    }
}
