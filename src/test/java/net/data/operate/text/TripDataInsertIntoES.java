package net.data.operate.text;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.data.operate.es.ElasticsearchUtil;
import net.data.operate.hdfs.HdfsOperate;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.Put;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-06-11
 * @desc
 */
@Slf4j
public class TripDataInsertIntoES {
    @Test
    public void insert() throws IOException, ParseException {
        HdfsOperate operate = new HdfsOperate();
        FSDataInputStream fin = operate.getReadInputStream("hdfs://namenode:8020/tianci/data/trip/trip_coord.csv");
        String res = null;
        BufferedReader r = new BufferedReader(new InputStreamReader(fin, "utf8"));
        List<Put> putList = new ArrayList<>();
        int count = 1;
        BulkRequestBuilder bulkRequest = ElasticsearchUtil.getClient().prepareBulk();

        while ((res = r.readLine()) != null) {
            res = res.replace("\"", "");
            String ress[] = res.split(",");
            if (ress.length <= 7) {
                continue;
            }
            count++;

            if (count % 10000 == 0) {
                log.info("es中上传" + count + "条数据");
                bulkRequest.execute().actionGet();
                bulkRequest = ElasticsearchUtil.getClient().prepareBulk();
            }

            JSONObject jobj = new JSONObject();
            DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
            DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = format1.parse(ress[1]);
            String time = format.format(date);
            jobj.put("rowKey", ress[0]);
            jobj.put("time", time);
            jobj.put("gps", ress[3] + "," + ress[4]);
            jobj.put("city", ress[7]);
            jobj.put("province", ress[8]);
            bulkRequest.add(ElasticsearchUtil.getClient().prepareIndex("trip_coord", "trip").setSource(jobj));
        }

    }

    public static void main(String[] args) {

    }

}
