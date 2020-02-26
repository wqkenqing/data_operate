package net.data.operate.spark;

import com.alibaba.fastjson.JSONObject;
import com.twitter.chill.Base64;
import net.data.operate.hbase.operate.HbaseOperate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-05-30
 * @desc
 */
public class SparkCount {
    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        return ScanToString;
    }

    public static void main(String[] args) throws IOException {

        String tname = args[0];
        String total = args[1];
        SparkConf con = new SparkConf().setAppName(tname + "_count");

        JavaSparkContext jsc = new JavaSparkContext(con);

        Scan scan = new Scan();
//        String tableName = "pbu:info_count_five";
        Configuration conf = HbaseOperate.getConf();
//        conf = HbaseOperate.initConfig("", conf);
        conf.set(TableInputFormat.INPUT_TABLE, tname);
        conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        JavaPairRDD hBaseRDD = jsc.newAPIHadoopRDD(conf,
                TableInputFormat.class, ImmutableBytesWritable.class,
                Result.class);
        long count = hBaseRDD.count();
        System.out.println(count);
        HbaseOperate operate = new HbaseOperate();
        JSONObject jsonObject = new JSONObject();
        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("count", count);
        jsonObject2.put("total", total);
        jsonObject.put(tname, jsonObject2);
        Put put = new Put((tname + "_total").getBytes());
        put.addColumn("info".getBytes(), "record".getBytes(), JSONObject.toJSONString(jsonObject).getBytes());
        operate.addPut(put, "test:data_change_record");

    }
}
