package net.data.operate.spark;

import net.data.operate.enums.GatherLogEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;
import util.RedisUtil;

import java.io.Serializable;
import java.util.*;

/**
 * 日志处理流程流
 *
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-08-19
 * @desc
 */
@Slf4j
public class LogOperateStream implements Serializable {
    //TODO 通过spark将 topic同步至hbase
    //TODO 需要添加过滤机制,用于过滤不需要同步的内容
    static String tmp = "tmp";
    static Jedis jedis = RedisUtil.getJedis();
    static final int DAYSECOND = 86400;

    /**
     * 如果12点则同步当天数据至总量key下。
     *
     * @param
     * @return
     * @throws
     * @author wqkenqing
     * @date 2019/10/22
     **/
    public static void checkTime(String hkey, String tag) {
        String suffix = "_TOTAL";
        String hkeyt = hkey + suffix;
        Map<String, String> dayMap = jedis.hgetAll(GatherLogEnum.createDay(hkey));
        Set<String> keySet = dayMap.keySet();
        try {
            for (String k : keySet) {
                String res = dayMap.get(k);
                if (tag.equals("undo")) {
                    jedis.hincrBy(hkeyt, k, Long.valueOf(res));
                    jedis.hset("LYDSJ_GATHER_DATA_INIT_JOB", "logstream_init", "done");
                } else {
                    jedis.hincrBy(hkeyt, k, Long.valueOf(1));
                }
                log.info("daily gains [{}] is completed....", k);
            }
        } catch (Exception e) {
            log.error("总量更新失败");
            e.printStackTrace();
        }
        jedis.close();

    }


    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf().setAppName("logstream");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        jssc.sparkContext().setLogLevel("INFO");
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "namenode:9092,datanode1:9092,datanode2:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("group.id", "log-consumer3");
        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add("logstream");

        JavaInputDStream<ConsumerRecord<String, String>> logDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));


        logDStream.mapPartitions(message -> {
//                    String stateKey = "gather_logerror_count";
//                    String countKey = "gather_log_count";
                    String errorKey = GatherLogEnum.createDay(GatherLogEnum.LYDSJ_GATHER_LOG_ERROR.getName());
                    String cerrorKey = GatherLogEnum.createDay(GatherLogEnum.LYDSJ_GATHER_CALL_ERROR.getName());
                    String countKey = GatherLogEnum.createDay(GatherLogEnum.LYDSJ_GATHER_LOG_COUNT.getName());
                    String ccountKey = GatherLogEnum.createDay(GatherLogEnum.LYDSJ_GATHER_CALL_COUNT.getName());

                    while (message.hasNext()) {
                        if (jedis == null) {
                            jedis = RedisUtil.getJedis();
                        }
                        String msg = message.next().value();
                        String[] msgs = msg.split("\\s");
                        System.out.println(msg);
                        String tag = msgs[2];
                        if (tag.equals("INFO")) {
                            String topic = msgs[4];
                            try {
                                jedis.hincrBy(countKey, topic, 1L);
                                jedis.hincrBy(ccountKey, topic, 1L);
                            } catch (Exception e) {
                                log.error("[{}]出错...", topic);
                                jedis.close();
                                jedis = RedisUtil.getJedis();
                            }
                        } else if (tag.equals("ERROR")) {
                            String topic = msgs[3];
                            jedis.hincrBy(errorKey, topic, 1L);
                            jedis.hincrBy(cerrorKey, topic, 1L);
                        }

                        //向total中添加增量
                        try {
                            String init_status = "done";
                            if (tmp.equals("tmp")) {
                                init_status = jedis.hget("LYDSJ_GATHER_DATA_INIT_JOB", "logstream_init");
                                tmp = "done";
                            }
                            checkTime(GatherLogEnum.LYDSJ_GATHER_LOG_COUNT.getName(), init_status);
                            checkTime(GatherLogEnum.LYDSJ_GATHER_LOG_ERROR.getName(), init_status);
                            checkTime(GatherLogEnum.LYDSJ_GATHER_CALL_ERROR.getName(), init_status);
                            checkTime(GatherLogEnum.LYDSJ_GATHER_CALL_COUNT.getName(), init_status);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (jedis != null) {
                            jedis.close();
                        }

                    }

                    return message;
                }
        ).

                print();
        jssc.start();
        jssc.awaitTermination();

    }
}
