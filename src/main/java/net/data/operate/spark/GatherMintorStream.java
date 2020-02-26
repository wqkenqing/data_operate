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
 * 采集状态监听流
 *
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-08-19
 */
@Slf4j
public class GatherMintorStream implements Serializable {
    //TODO 通过spark将 topic同步至hbase
    //TODO 需要添加过滤机制,用于过滤不需要同步的内容


    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf().setAppName("statestream");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        jssc.sparkContext().setLogLevel("INFO");
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "namenode:9092,datanode1:9092,datanode2:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("group.id", "log-consumer2");
        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add("gather_serivce_state");
        String key = GatherLogEnum.LYDSJ_GATHER_LOG_STATE.getName();
        JavaInputDStream<ConsumerRecord<String, String>> logDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

        logDStream.mapPartitions(message -> {
                    Map<String, String> map = new HashMap<>();
                    Jedis jedis = RedisUtil.getJedis();
                    if (jedis.hgetAll(key) == null) {
                        jedis.hmset(key, map);
                    } else {
                        map = jedis.hgetAll(key);
                    }
                    while (message.hasNext()) {
                        String msg = message.next().value();
                        String[] msgs = msg.split("\\s");
                        String time = msgs[0] + " " + msgs[1];
                        String topic = msgs[4];
                        jedis.hset(GatherLogEnum.LYDSJ_GATHER_LOG_STATE_TIME.getName(), topic, time);
                    }
                    log.info("info upload...");
                    jedis.close();
                    return message;
                }
        ).print();
        jssc.start();
        jssc.awaitTermination();

    }
}
