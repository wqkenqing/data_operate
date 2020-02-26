package net.data.operate.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

/**
 * 数据同步至hbase
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-08-19
 */
@Slf4j
@Deprecated
public class KafkaTopicToHbase {
    //TODO 通过spark将 topic同步至hbase
    //TODO 需要添加过滤机制,用于过滤不需要同步的内容

    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
        jssc.sparkContext().setLogLevel("INFO");
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("metadata.broker.list", "namenode:9092");
        kafkaParams.put("auto.offset.reset", "smallest");
        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add("logstream");

        JavaInputDStream<ConsumerRecord<String, String>> logDStream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

        logDStream.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
