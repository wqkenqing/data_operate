package net.data.operate.text;

import lombok.extern.slf4j.Slf4j;
import net.data.operate.es.ElasticsearchUtil;
import net.data.operate.hbase.operate.HbaseOperate;
import net.data.operate.util.CommonUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.junit.Test;
import org.mortbay.log.Log;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-06-10
 * @desc 航班信息表导入hbase, 并通过es建立二级索引
 */
@Slf4j
public class FlightDemo {
    static HbaseOperate hbaseOperate = new HbaseOperate();

    public void insertInfoToHbase(String filepath, String tablename) {

    }

    public void createFlightTable(String tname, String[] args) throws IOException {
        hbaseOperate.createTable(tname, args);
    }

    /**
     * 往hbase中导入数据
     */
    public void importDataToHbase() throws IOException {

        FlightDemo flight = new FlightDemo();
        String path = "/Users/wqkenqing/Desktop/flight_info.txt";
        String flith_info = CommonUtil.stream2String(new FileInputStream(path), "utf8");
        List<Put> putList = new ArrayList<>();
        Arrays.asList(flith_info.split("\n")).forEach(f -> {
            String ff[] = f.split("\t");
            String airplane_id = ff[8];
            String flight_number = ff[3];
            String flight_id = ff[0];
            String rowkey = airplane_id + "_" + flight_number + "_" + flight_id;
            Put put = new Put(rowkey.getBytes());
            put.addColumn("info".getBytes(), "importance".getBytes(), ff[ff.length - 1].getBytes());
            put.addColumn("info".getBytes(), "start_time".getBytes(), ff[ff.length - 5].getBytes());
            put.addColumn("info".getBytes(), "ent_time".getBytes(), ff[ff.length - 4].getBytes());
            putList.add(put);
        });
        flight.hbaseOperate.insertList("flight_info", putList);
    }

    /**
     * 从es中获得符合条件的rowkeyList
     */
    public List<Get> getRowKeyListFromES(QueryBuilder queryBuilder) {

        List<Get> rlist = new ArrayList<>();
        List<Map<String, Object>> res1 = ElasticsearchUtil.searchListData("traffic", "fly", queryBuilder, 10000, "", "", "");
        res1.forEach(s -> {
                    String val = (String) s.get("rowKey");
                    rlist.add(new Get(val.getBytes()));
                }
        );
        return rlist;
    }

    public Result[] getResultFromHbase(String tname, List<Get> rlist) throws IOException {


        Result res[] = hbaseOperate.getPutList(tname, rlist);

        return res;
    }

    /**
     * 通过demo1,demo2对比测试通过es获得rowkeylist再去获取对应row信息与直接通过scan+filter形式来获取信息的效率.
     */
    @Test
    public void queryDemo1() throws IOException {
//        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("importance"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0"));
        Filter efilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("importance"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2.0"));
        FilterList flist = new FilterList();
//        flist.addFilter(filter);
        flist.addFilter(efilter);
        LocalTime start = LocalTime.now();

        List<Result> rlist = hbaseOperate.scanByFilter("flight_info", flist);

        LocalTime stop = LocalTime.now();
        long res = Duration.between(start, stop).toMillis();
        log.info("总共" + rlist.size());
        log.info("耗时" + res);
    }

    @Test
    public void queryDemo2() throws IOException {
//        MatchPhraseQueryBuilder mq = QueryBuilders.matchPhraseQuery("importance", "1.71");
        RangeQueryBuilder rg = QueryBuilders.rangeQuery("importance").from("0").to("2.0");
//        RangeQueryBuilder rg = QueryBuilders.rangeQuery("importance").from("0");

        FlightDemo flight = new FlightDemo();

        String tname = "flight_info";

        List<Get> rlist = flight.getRowKeyListFromES(rg);

        Log.info("rlist获取完毕");
        LocalTime start = LocalTime.now();
        Result[] res = flight.getResultFromHbase(tname, rlist);
        LocalTime stop = LocalTime.now();

        long duration = Duration.between(start, stop).toMillis();
        log.info("获得" + res.length + "条记录");
        log.info("总耗时" + duration);

    }
}
