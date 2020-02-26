package net.data.operate.text;

import lombok.extern.slf4j.Slf4j;
import net.data.operate.es.ElasticsearchUtil;
import net.data.operate.hbase.operate.HbaseOperate;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.mortbay.log.Log;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-06-12
 * @desc 对trip_coord表的查询效率比较
 */
@Slf4j
public class TripDemo {
    static HbaseOperate hbaseOperate = new HbaseOperate();

    /**
     * 从es中获得符合条件的rowkeyList
     */
    public List<Get> getRowKeyListFromES(QueryBuilder queryBuilder, int number) {

        List<Get> rlist = new ArrayList<>();
        List<Map<String, Object>> res1 = ElasticsearchUtil.searchListData("trip_coord", "trip", queryBuilder, number, "", "", "");
        res1.forEach(s -> {
                    String val = (String) s.get("rowKey");
                    Get get = new Get(val.getBytes());
                    rlist.add(get);
                }
        );
        return rlist;
    }

    public Result[] getResultFromHbase(String tname, List<Get> rlist) throws IOException {

        Result res[] = hbaseOperate.getPutList(tname, rlist);

        return res;
    }

    @Test
    public void queryDemo2() throws IOException, InterruptedException {
        MatchPhraseQueryBuilder mq = QueryBuilders.matchPhraseQuery("province", "沧州市");
//        RangeQueryBuilder rg = QueryBuilders.rangeQuery("importance").from("0").to("2.0");
//        RangeQueryBuilder rg = QueryBuilders.rangeQuery("").from("0");


        TripDemo trip = new TripDemo();

        String tname = "trip_coord";

        List<Get> rlist = trip.getRowKeyListFromES(mq, 80000);

        Log.info("rlist获取完毕")
        ;
        LocalTime start = LocalTime.now();
        Set<Result> results = mutiGet(rlist, hbaseOperate);

//        Result[] res = trip.getResultFromHbase(tname, rlist);

        LocalTime stop = LocalTime.now();

        long duration = Duration.between(start, stop).toMillis();
        log.info("获得" + results.size() + "条记录");
        log.info("总耗时" + duration);

    }


    public Set<Result> mutiGet(List<Get> getList, HbaseOperate operate) throws IOException, InterruptedException {

        // 每500条数据开启一条线程
        int threadSize = 200;
        // 总数据条数
        int dataSize = getList.size();
        // 线程数
        int threadNum = dataSize / threadSize + 1;
        // 定义标记,过滤threadNum为整数
        boolean special = dataSize % threadSize == 0;
        // 创建一个线程池
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        // 定义一个任务集合
        List<Callable<Result[]>> tasks = new ArrayList<Callable<Result[]>>();
        Callable<Result[]> task = null;
        List<Get> cutList = null;
        int times = 1;
        // 确定每条线程的数据
        for (int i = 0; i < threadNum; i++) {
            if (i == threadNum - 1) {
                if (special) {
                    break;
                }
                cutList = getList.subList(threadSize * i, dataSize);
            } else {
                cutList = getList.subList(threadSize * i, threadSize * (i + 1));
            }
            final List<Get> listStr = cutList;
            task = (() -> {
                return hbaseOperate.getPutList("trip_coord", listStr);
            });

            tasks.add(task);
        }

        HashSet<Result> resList = new HashSet<>();

        List<Future<Result[]>> results = exec.invokeAll(tasks);
        for (Future<Result[]> future : results) {
            try {
                if (future.get() == null) {
                    log.info("future为null");
                }
                Result[] lstr = future.get();
                if (lstr.length > 0) {
                    resList.addAll(Arrays.asList(lstr));
                }
            } catch (Exception e) {
            }
        }
        return resList;
    }

    @Test
    public void queryDemo1() throws IOException {
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("province"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("沧州市"));
//        Filter efilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("importance"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2.0"));
        FilterList flist = new FilterList();
        flist.addFilter(filter);
//        flist.addFilter(efilter);
        LocalTime start = LocalTime.now();

        List<Result> rlist = hbaseOperate.scanByFilter("trip_coord", flist);
        log.info("获得" + rlist.size() + "条数据");
        LocalTime stop = LocalTime.now();
        long res = Duration.between(start, stop).toMillis();
        log.info("总共" + rlist.size());
        log.info("耗时" + res);
    }

}

