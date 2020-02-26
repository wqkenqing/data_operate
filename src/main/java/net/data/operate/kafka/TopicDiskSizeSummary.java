package net.data.operate.kafka;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-08-06
 * @desc
 */


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
@Deprecated
public class TopicDiskSizeSummary {
    private static AdminClient admin;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String brokers = "192.168.10.100:9092";
        initialize(brokers);
        try {
            long topic1InBroker0 = getTopicDiskSizeForSomeBroker("test_topic", 0);
            long topic2InBroker1 = getTopicDiskSizeForSomeBroker("test_topic", 1);
            long topic2InBroker2 = getTopicDiskSizeForSomeBroker("test_topic", 2);
            System.out.println(topic1InBroker0);
            System.out.println(topic2InBroker1);
            System.out.println(topic2InBroker2);
        } finally {
            shutdown();
        }
    }

    public static long getTopicDiskSizeForSomeBroker(String topic, int brokerID)
            throws ExecutionException, InterruptedException {
        long sum = 0;
        DescribeLogDirsResult ret = admin.describeLogDirs(Collections.singletonList(brokerID));
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> tmp = ret.all().get();
        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> entry : tmp.entrySet()) {
            Map<String, DescribeLogDirsResponse.LogDirInfo> tmp1 = entry.getValue();
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> entry1 : tmp1.entrySet()) {
                DescribeLogDirsResponse.LogDirInfo info = entry1.getValue();
                Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoMap = info.replicaInfos;
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : replicaInfoMap.entrySet()) {
                    if (topic.equals(replicas.getKey().topic())) {
                        sum += replicas.getValue().size;
                    }
                }
            }
        }
        return sum;
    }

    private static void initialize(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        admin = AdminClient.create(props);
        System.out.println("加载结束....");
    }

    private static void shutdown() {
        if (admin != null) {
            admin.close();
        }
    }
}
