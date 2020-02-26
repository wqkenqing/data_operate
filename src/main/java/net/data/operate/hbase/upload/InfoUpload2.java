package net.data.operate.hbase.upload;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.data.operate.hbase.base.InfoBaseUpload;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-22
 * insert mock data into hbase
 */
@Slf4j
public class InfoUpload2 extends InfoBaseUpload {

    @Override
    public Put packageInfoPut(JSONObject jobj) {
        long rowkey = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(rowkey));
        String res = JSONObject.toJSONString(jobj);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("res"), Bytes.toBytes(res));
        return put;
    }

    public static void main(String[] args) {
//        String toppicName = args[0];
//        String toppicName = "publicbusiness-monitorypoint-camera_traffic_event";

        String toppicName = "pto-travel-forcast";
        InfoUpload2 upload = new InfoUpload2();
        upload.infoUploadToHbase(toppicName);

    }
}
