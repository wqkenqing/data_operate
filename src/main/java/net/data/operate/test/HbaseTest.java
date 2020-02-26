package net.data.operate.test;

import net.data.operate.hbase.operate.HbaseOperate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-09-03
 * @desc
 */
public class HbaseTest {
    public static void main(String[] args) throws IOException {
        HbaseOperate operate = new HbaseOperate();

        List<Put> plist = new ArrayList<>();
        for (int i = 10; i <21 ; i++) {
            Put put = new Put(Bytes.toBytes("key" + i));
            put.addColumn("info".getBytes(), "res".getBytes(), "dssdd".getBytes());

            plist.add(put);
        }

        operate.addPutList(plist,"pto:sse1");
    }
}
