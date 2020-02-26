package net.data.operate.hbase.upload;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.data.operate.hbase.base.InfoBaseUpload;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-22
 * insert mock data into hbase
 */
@Slf4j
public class TrafficInfoNewUpload extends InfoBaseUpload {

    @Override
    public Put packageInfoPut(JSONObject jobj) {
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

    public static void main(String[] args) {
//        String toppicName = args[0];
//        String toppicName = "publicbusiness-monitorypoint-camera_traffic_event";

        String toppicName = "pbu-person-info_count_five";
        TrafficInfoNewUpload upload = new TrafficInfoNewUpload();
        upload.infoUploadToHbase(toppicName);

    }
}
