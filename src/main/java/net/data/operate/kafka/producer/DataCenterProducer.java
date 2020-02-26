package net.data.operate.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import net.data.operate.util.CommonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-03-08
 * @desc
 */

@Slf4j
public class DataCenterProducer {
    static Properties props = new Properties();
    static KafkaProducer<String, String> producer;

    public static void setConf(String address) {
        props.put("bootstrap.servers", address);
        //acks=0：如果设置为0，生产者不会等待kafka的响应。
        //acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        //acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这是最强的可用性保证。
        props.put("acks", "1");
        //配置为大于0的值的话，客户端会在消息发送失败时重新发送。
        props.put("retries", 0);
        //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高client和生产者的效率
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
    }

    public static void produceSend(String topic, String val) throws InterruptedException {
        producer.send(new ProducerRecord<String, String>(topic, val));
    }


    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

//        DataCenterProducer.setConf("namenode:9092,datanode1:9092,datanode2:9092");
        DataCenterProducer.setConf("192.168.10.211:9092");
        String topic = "jzw_toll_island_state";
        String state = CommonUtil.stream2String(new FileInputStream("/Users/wqkenqing/Desktop/yg_doc/state_all.txt"), "utf8");
        String[] sts = state.split("\n");
        for (String st:sts
             ) {
            DataCenterProducer.produceSend(topic, st);
        }

    }
}
