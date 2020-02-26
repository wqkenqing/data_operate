package net.data.operate.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-22
 * @desc
 */
public class SeekFromBegining implements ConsumerRebalanceListener {
    Consumer<String, String> consumer;

    public  SeekFromBegining(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        Map<TopicPartition, Long> beginningOffset = consumer.beginningOffsets(collection);

        //读取历史数据 --from-beginning
        for (Map.Entry<TopicPartition, Long> entry : beginningOffset.entrySet()) {
            // 基于seek方法
            //TopicPartition tp = entry.getKey();
            //long offset = entry.getValue();
            //consumer.seek(tp,offset);
            // 基于seekToBeginning方法
            consumer.seekToBeginning(collection);
        }
    }
}
