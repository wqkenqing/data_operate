package net.data.operate.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @className: AutoMappingKeyInterceptor
 * @author: wqkenqing
 * @date: 2021/5/7 10:45 上午
 * @desc 通过提取json对象中的字段属性实现自动映射kafka topic 中的key
 **/
@Slf4j
public class AutoMappingKeyInterceptor implements Interceptor {
    private final boolean preserveExisting;
    //指定的json中用来作为topic key的属性名
    private final String key;

    public AutoMappingKeyInterceptor(boolean preserveExisting, String key) {
        this.preserveExisting = preserveExisting;
        this.key = key;
    }

    @Override
    public void initialize() {
        //pass
        log.debug("enter into AutoMappingKeyInterceptor ....");
    }

    /**
     * @desc: 通过解析event body 内容(json字符串),添加类型为key的属性
     * @date: 2021/5/7
     **/
    @Override
    public Event intercept(Event event) {
        //获取header对象
        //获取在配置文件中指定的key
        //通过指定的key,获取对应的值，作为新键key的值
        log.debug("begin to operate the data !");
        //获取body
        String body = new String(event.getBody(), Charsets.UTF_8);
        log.info("body info {}", body);
        Map<String, String> header = event.getHeaders();
        try {
            if (body.contains("{") && body.contains("}")) {
                log.debug("begint to parse");
                JSONObject bodyJson = JSONObject.parseObject(body);
                log.debug("parse end!");
                String value = (String) bodyJson.get(key);
                if (StringUtils.isNotBlank(value) && StringUtils.isNotEmpty(value)) {
                    header.put("key", value);
                    event.setHeaders(header);
                    log.debug("the data has success parse into json ");
                    return event;
                } else {
                    log.error("the key [{}] is not exist ,add topic's key is failed!", key);
                    return event;
                }
            } else {
                log.debug("the body info don't contains json");
                return event;
            }

        } catch (Exception e) {
            log.error("json parse has some wrong info");
            return event;
        }

    }

    /**
     * @desc: 批量处理events
     * @date: 2021/5/7
     **/
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> eventList = new ArrayList<>();
        for (Event event : list) {
            //获取body
            Event eventNew = intercept(event);
            eventList.add(eventNew);
        }

        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private boolean preserveExisting;
        private String key;

        @Override
        public Interceptor build() {
            return new AutoMappingKeyInterceptor(this.preserveExisting, this.key);
        }

        @Override
        public void configure(Context context) {
            this.preserveExisting = context.getBoolean("preserveExisting", true);
            this.key = context.getString("key", "key");
        }
    }
}
