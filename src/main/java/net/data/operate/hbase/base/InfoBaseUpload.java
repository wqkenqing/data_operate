package net.data.operate.hbase.base;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.data.operate.hbase.operate.HbaseOperate;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import util.KafkaUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-22
 * insert mock data into hbase
 */
@Slf4j
public abstract class InfoBaseUpload {
    static HbaseOperate operate;
    KafkaConsumer<String, String> consumer;

    static String HBASE_META_TABLE = "test:table_meta";
    static String META_COLLECT_SOURCE = "test:collect_source";

    static String HY_TRAFFIC_INFO = "test:hy_traffic_info";

    static String META_VALUE = "true"; //true表示在启用,false表示已停用
    static String KAFKA_ADRESS = "namenode:9092"; //true表示在启用,false表示已停用

    static String[] FAMILY = new String[]{"info"};

    static {
        operate = new HbaseOperate();
    }


    public KafkaConsumer<String, String> getConsumer() {
        consumer = KafkaUtil.createConsumer(KAFKA_ADRESS);
        return consumer;
    }
    public KafkaConsumer<String, String> getConsumer(String topicName) {
        consumer = KafkaUtil.createConsumer(KAFKA_ADRESS,topicName);
        return consumer;
    }


    /**
     * 将消息对象转成相应的Put
     */
    public abstract Put packageInfoPut(JSONObject jobj);


    /**
     * 方法的功能描述
     *
     * @param
     * @return
     * @throws
     * @author wqkenqing
     * @date 2019-07-23
     * @desc
     **/
    public void infoUploadToHbase(String topicName) {
        consumer = getConsumer(topicName);
        consumer.subscribe(Collections.singleton(topicName));
        List<Put> plist = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.records(topicName).forEach(record -> {
                String val = record.value();
                JSONObject jobj = JSONObject.parseObject(val);
                String tableName = topicNameResolve(topicName, jobj);
                Put put = packageInfoPut(jobj);

                if (put != null) {
                    plist.add(put);
                }
                //组装putList,累计30条上传
                if (plist.size() == 30) {
                    operate.addPutList(plist, tableName);
                    plist.clear();
                    if (plist.size() != 0) {
                        log.warn("plist未清空,注意检查");
                    }
                }
            });
        }
    }

    /**
     * 向元数据空间添加相关信息
     *
     * @param jobj
     * @param topicName
     * @return
     * @author wqkenqing
     * @date 2019-08-12
     **/

    public void insertMetaDataIntoHbase(JSONObject jobj, String topicName) {
        Set<String> set = jobj.keySet();

        if (operate == null) {
            operate = new HbaseOperate();
        }

        Put put = new Put(topicName.getBytes());

        for (String key : set) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(META_VALUE));
        }

        operate.addPut(put, HBASE_META_TABLE);
    }

    public void instertMetaSourceIntoHbase(String topicName) {

    }

    /**
     * topic映射要求
     *
     * @param topicName
     * @param jobj
     * @return
     * @throws
     * @author wqkenqing
     **/
    public String topicNameResolve(String topicName, JSONObject jobj) {
        String[] names = topicName.split("-");
        String namespace = names[0];
        String source = names[1];
        String tableNamePart = names[2];
        String tableName = namespace + ":" + tableNamePart;
        if (names.length >= 3 || topicName.equals("publicbusiness-monitorypoint-camera_traffic_event")) {

            String result = operate.getRow(HBASE_META_TABLE, tableName);
            String sourceR = operate.getRow(META_COLLECT_SOURCE, tableName);

            if (StringUtils.isEmpty(sourceR) || StringUtils.isEmpty(sourceR)) {
                Put put = new Put(tableName.getBytes());
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source"), Bytes.toBytes(source));
                operate.addPut(put, META_COLLECT_SOURCE);
            }

            if (!StringUtils.isBlank(result) || !StringUtils.isEmpty(result)) {

                //已经加载过元数据

            } else {

                //创建hbase表,并添加元素据表
                if (operate == null) {
                    operate = new HbaseOperate();
                }

                boolean res = operate.tableIsExist(tableName);
                if (!res) {
                    try {
                        operate.createTable(tableName, FAMILY);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                //添加元数据
                Set<String> set = jobj.keySet();
                Put put = new Put(tableName.getBytes());

                for (String key : set) {
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key), Bytes.toBytes(META_VALUE));
                }
                operate.addPut(put, HBASE_META_TABLE);
                log.info("元数据和表[{}]初始化成功", topicName);

                //添加数据来源
                Put putn = new Put(tableName.getBytes());

                putn.addColumn(Bytes.toBytes("info"), Bytes.toBytes("source"), Bytes.toBytes(source));

                operate.addPut(putn, META_COLLECT_SOURCE);

                log.info("数据源[{}]同步成功", topicName);

            }
        }
        return tableName;
    }


}
