package net.data.operate.hbase.operate;


import lombok.extern.slf4j.Slf4j;
import net.data.operate.util.CommonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-05-17
 * @desc hbase常规操作封装
 */

@Slf4j
public class HbaseOperate {
    //1. configruation 配置  2.所有表 3.创建表 4.删除表 5.修改表 6.添加信息 7.读取信息 8.删除信息
    private static Configuration conf = null;
    private static HBaseAdmin hAdmin = null;
    public static HTable table = null;
    private static Properties properties;

    static {
        properties = CommonUtil.initConfig("hbase.properties");
        conf = HBaseConfiguration.create();
        //1. 设置zookeeper集群 2.端口号 3.hdfs上的hbase地址 4.nameservices
        for (Object k : properties.keySet()) {
            String key = String.valueOf(k);
            conf.set(key, String.valueOf(properties.getProperty(key)));
        }
        try {
            hAdmin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HTable getTable(String tableName) {
         table = null;
        try {
            table = new HTable(conf, tableName);
        } catch (Exception e) {
            log.error("htable获取失败");
        }
        return table;
    }


    public static Configuration getConf() {
        return conf;
    }

    /**
     * 罗列所有表
     */
    public void listAllTables() {
        try {
            HBaseAdmin hAdmin = new HBaseAdmin(conf);
            log.info("以下是所有的表...");
            Arrays.asList(hAdmin.listTableNames()).forEach(t -> {
                System.out.println(t.toString());
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表,并且要有表的描述
     **/
    public void createTable(String tableName, String[] columnFamilys) throws IOException {
        if (hAdmin.tableExists(tableName)) {
            log.info("表已存在,请不要重复创建..");
            return;
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        Arrays.asList(columnFamilys).forEach(c -> {
            desc.addFamily(new HColumnDescriptor(c.getBytes()));
        });
        try {
            if (hAdmin == null) {

                hAdmin = new HBaseAdmin(conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        hAdmin.createTable(desc);
        log.info(tableName + ":创建成功....");
    }

    /**
     * 删除表
     */
    public void deleteTable(String tableName) throws IOException {
        if (hAdmin == null) {
            hAdmin = new HBaseAdmin(conf);
        }
        hAdmin.disableTable(tableName);
        hAdmin.deleteTable(tableName);
        log.info(tableName + ":删除成功....");

    }

    public void addRow(String tableName, String row, String columnFamily, String column, String val) {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(val));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 多版本version Get
     */
    public void getRowShow(String tableName, String row) {
        try {
            HTable table = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(row));

            get.setMaxVersions(3);
            Result res = table.get(get);
            res.listCells().forEach(r -> {

                System.out.println(Bytes.toString(r.getFamily()));
                System.out.println(Bytes.toString(r.getQualifier()));
                System.out.println(Bytes.toString(r.getValue()));
                System.out.println(r.getTimestamp());

            });
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public String getRow(String tableName, String row) {
        String result = "";

        try {
            HTable table = new HTable(conf, tableName);
            Get get = new Get(Bytes.toBytes(row));

            Result res = table.get(get);
            if (res == null) {
                return "";
            }
            result = Bytes.toString(res.listCells().get(0).getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * scan
     */
    public void scanShow(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        scan.setMaxVersions(3);

        scanner.next().listCells().forEach(row -> {
            System.out.println(Bytes.toString(row.getFamily()));
            System.out.println(Bytes.toString(row.getQualifier()));
            System.out.println(Bytes.toString(row.getValue()));
            System.out.println(row.getTimestamp());
        });
    }

    public void deleteRow(String tableName, String row) {
        try {
            HTable table = new HTable(conf, tableName);
            table.delete(new Delete(row.getBytes()));
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 通过rowkeyList获得,在大批量操作的时候能有效率的提升
     */
    public Result[] getPutList(String tableName, List<Get> rlist) throws IOException {

        HTable table = new HTable(conf, tableName);
        LocalTime start = LocalTime.now();
        Result[] rset = table.get(rlist);
        LocalTime stop = LocalTime.now();
        Duration duration = Duration.between(start, stop);
        log.info("耗时:" + duration.toMillis());
        return rset;
    }


    /**
     * 批量写入
     */
    public void insertList(String tname, List<Put> putList) throws IOException {
        HTable table = new HTable(conf, tname);
        try {
            table.put(putList);
            table.close();
        } catch (Exception e) {
            log.warn("批量写入失败");
            return;
        }
        log.info("批量写入成功...");
    }


    public List<Result> scanByFilter(String tableName, FilterList filterList) throws IOException {

        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.setCaching(10000);
//        scan.setBatch(1000);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> results = new ArrayList<>();
        Result res = null;
        int count = 1;
        while ((res = scanner.next()) != null) {
            results.add(res);
            count++;
            if (count == 100000) {
                return results;
            }
        }
//        scanner.iterator().forEachRemaining(result -> {
//            results.add(result);
//        });
//        return results;
        return results;
    }


    public static void main(String[] args) throws FileNotFoundException {
        HbaseOperate operate = new HbaseOperate();
        HbaseOperate.initConfig("", new Configuration());
    }

    /**
     * Hbase单条记录添加
     *
     * @param put
     * @param tableName
     * @return
     * @throws
     * @author wqkenqing
     * @date 2019-07-23
     **/
    public void addPut(Put put, String tableName) {
        HTable table = null;
        try {
            table = new HTable(conf, tableName);
            table.put(put);
            table.flushCommits();
            table.close();
        } catch (Exception e) {
            log.error("[{}]表添加记录失败", tableName);
            e.printStackTrace();
            return;
        }
        log.info("[{}]表添加记录成功", tableName);
    }

    /**
     * 批量提交putlist
     *
     * @param plist
     * @param tableName
     * @return
     * @throws
     * @author wqkenqing
     * @date 2019-07-23
     **/
    public void addPutList(List<Put> plist, String tableName) {
        try {
            getTable(tableName);
            if (table == null) {
                table = getTable(tableName);
            }
            table.put(plist);
            table.flushCommits();

            log.info("[{}]成功上传[{}]条数据", tableName, plist.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public boolean tableIsExist(String tname) {

        boolean res = false;
        try {
            HBaseAdmin hAdmin = new HBaseAdmin(conf);
            res = hAdmin.tableExists(tname.getBytes());
        } catch (IOException e) {
            e.printStackTrace();

        }
        return res;
    }

    public static Configuration initConfig(String path, Configuration conf) throws FileNotFoundException {
        path = HbaseOperate.class.getResource("/hbase.properties").getPath();
        System.out.println(path);
        File dumpFile = new File(path);
        Map prop = Yaml.loadType(dumpFile, HashMap.class);
        Map<String, Object> propn = prop;
        for (String key : propn.keySet()) {
            conf.set(key, (String) propn.get(key));
        }
        return conf;
    }


}
