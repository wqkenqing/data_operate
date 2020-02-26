package net.data.operate.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-05-08
 * @desc
 */
public class AtMostOnceConsumer {

    public static void main(String[] str) throws Exception {
        System.out.println("Starting AtMostOnceConsumer ...");
        execute();
    }

    private static void execute() throws Exception {
        KafkaConsumer<String, String> consumer = createConsumer();
// Subscribe to all partition in that topic. 'assign' could be used here
// instead of 'subscribe' to subscribe to specific partition.
//        consumer.subscribe(Collections.singleton("carstream"));
        consumer.subscribe(Collections.singleton("traffic_five_minutes_current_time"));
        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.100:9092");
        String consumeGroup = "cg1";
        props.put("group.id", consumeGroup);
// Set this property, if auto commit should happen.
        props.put("enable.auto.commit", "true");
// Auto commit interval, kafka would commit offset at this interval.
        props.put("auto.commit.interval.ms", "101");
// This is how to control number of records being read in each poll
        props.put("max.partition.fetch.bytes", "135");
// Set this if you want to always read from beginning.
// props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) throws Exception {
        while (true) {
            ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, String> record : records.records("pbu-yidong-tourist_minuteLocal")) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                lastOffset = record.offset();
                consumer.commitSync(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
            }
            System.out.println(" lastOffset read: " + lastOffset);
            process();
        }
    }

    private static void process() throws InterruptedException {
// create some delay to simulate processing of the message.
        Thread.sleep(20);
    }

}
