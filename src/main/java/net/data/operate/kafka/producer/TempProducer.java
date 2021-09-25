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
public class TempProducer {
    static Properties props = new Properties();
    static KafkaProducer<String, String> producer;

    public static void setConf(String address) {
        props.put("bootstrap.servers", address);
        //acks=0：如果设置为0，生产者不会等待kafka的响应。
        //acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        //acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这是最强的可用性保证。
        props.put("acks", "0");
        //配置为大于0的值的话，客户端会在消息发送失败时重新发送。
        props.put("retries", 0);
        //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高client和生产者的效率
        props.put("batch.size", 163840);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
    }

    public static void produceSend(String topic, String val) throws InterruptedException {
        producer.send(new ProducerRecord<String, String>(topic, val));
    }


    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
//        String file = args[0];
        String file = "/Users/kuiqwang/yaobo.log";
        TempProducer.setConf("kafka01:9092");
        String topic = "jllsd-flume-collect-from-yaobo";
        String recordInfo = CommonUtil.stream2String(new FileInputStream(file), "utf8");
        String[] records = recordInfo.split("\n");
        int i = 0;
        while (i <= records.length - 1) {
            TempProducer.produceSend(topic, records[i]);
            if (i == records.length - 1) {
                Thread.sleep(5000);
            }
            i++;

        }
    }
}
