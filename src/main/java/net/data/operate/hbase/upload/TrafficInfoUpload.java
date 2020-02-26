package net.data.operate.hbase.upload;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.data.operate.hbase.operate.HbaseOperate;
import net.data.operate.kafka.SeekFromBegining;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import util.KafkaUtil;

import java.util.*;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-22
 * insert mock data into hbase
 */

@Slf4j
public class TrafficInfoUpload {

    static HbaseOperate operate;
    KafkaConsumer<String, String> consumer;

    static String HBASE_META_TABLE = "test:table_meta";

    static String HY_TRAFFIC_INFO = "test:hy_traffic_info";

    static String META_VALUE = "true"; //true表示在启用,false表示已停用

    static {
        operate = new HbaseOperate();
    }


    public KafkaConsumer<String, String> getConsumer() {

        String address = "";
        consumer = KafkaUtil.createConsumer(address);
        return consumer;

    }


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
//        String  = "traffic_info";
        consumer = getConsumer();
        consumer.subscribe(Collections.singleton(topicName), new SeekFromBegining(consumer));
        Map<String, Integer> resMap = new HashMap<>();
        resMap.put("init", 0);
        resMap.put("meta", 0);
        List<Put> plist = new ArrayList<>();
        String tname = "test:hy_traffic_info";
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            records.records(topicName).forEach(record -> {
                String val = record.value();
                JSONObject jobj = JSONObject.parseObject(val);

                //初始化元数据表
                if (resMap.get("meta") == 0) {
                    insertMetaDataIntoHbase(jobj, tname);
                    resMap.put("meta", 1);
                    return;
                }
                Put put = packageTrafficInfoPut(jobj);
                if (put != null) {
                    plist.add(put);
                }
                //组装putList,累计30条上传
                if (plist.size() == 30) {
                    operate.addPutList(plist, HY_TRAFFIC_INFO);
                    plist.clear();
                    log.info("批次上次成功");
                    if (plist.size() != 0) {
                        log.warn("plist未清空,注意检查");
                    }
                }
            });
        }
    }

    /**
     * 将消息对象转成相应的Put
     */
    public Put packageTrafficInfoPut(JSONObject jobj) {
        String recordTime = "";
        try {
            recordTime = (String) jobj.get("recordTime");
        } catch (Exception e) {
            return null;
        }
        String licenceNumber = (String) jobj.get("licenceNumber");
        licenceNumber = licenceNumber.replace(" ", "");

        String rowkey = "";
        if (StringUtils.isBlank(recordTime) || !recordTime.equals("null") || !recordTime.equals("NULL")) {
            rowkey = licenceNumber + "_" + recordTime;
        } else {
            return null;
        }
        Put put = new Put(Bytes.toBytes(rowkey));
        String res = JSONObject.toJSONString(jobj);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("res"), Bytes.toBytes(res));
        return put;
    }


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

    public static void main(String[] args) {
        String topicName = "traffic_info";
        TrafficInfoUpload upload = new TrafficInfoUpload();
        upload.infoUploadToHbase(topicName);
    }
}
