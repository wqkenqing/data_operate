package net.data.operate.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @className: HbaseTemplateMR
 * @author: kuiqwang
 * @date: 2019/1/8 9:55 AM
 * @describe: hbase to hbase
 **/

public class HbaseRowCount implements Tool {
    static String FILE_ROOT = "";
    static String FILE_INPUT = "";
    static String FILE_OUTPUT = "";

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new HbaseRowCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
//        FILE_ROOT = args[0];
//        FILE_INPUT = args[1];
//        FILE_INPUT = args[0];
//        FILE_OUTPUT = args[2];
        FILE_ROOT = "/";
        FILE_INPUT = "meta:topics";
        FILE_OUTPUT = "/Users/wqkenqing/Desktop/yg_code/data_operate/out3";

//        if (!FILE_OUTPUT.contains(basename)) {
//            System.exit(-1);
//        }

        Configuration conf = new Configuration();
        //设置zookeeper
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //设置hbase表名称
        conf.set(TableInputFormat.INPUT_TABLE, FILE_INPUT);
        Scan scan = new Scan();

        //将该值改大，防止hbase超时退出

        // 0 定义干活的人
        Job job = new Job(conf);
        job.setJobName("rowkeyoperate");
        TableMapReduceUtil.initTableMapperJob(FILE_INPUT, scan, HbaseRowCountMapper.class, Text.class, Text.class, job);


//        FileOutputFormat.setOutputPath(job, outpath);
//        job.setOutputFormatClass(TextOutputFormat.class);


        FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT));
        // 打包运行必须执行的方法
        job.setJarByClass(HbaseRowCount.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}

class HbaseRowCountMapper extends TableMapper<Text, Text> {


    {

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        JSONObject jobj;
        try {
            for (Cell cell : value.rawCells()) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                context.write(new Text(rowkey), new Text(""));
            }
        } catch (Exception e) {
            System.out.println("出错了....");
        }
    }
}

