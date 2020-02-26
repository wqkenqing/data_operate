package net.data.operate.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @className: HbaseTemplateMR
 * @author: kuiqwang
 * @date: 2019/1/8 9:55 AM
 * @describe: hbase to hbase
 **/
public class CarmeraHbaseMR implements Tool {
    static String FILE_ROOT = "";
    static String FILE_INPUT = "";
    static String FILE_OUTPUT = "";

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new CarmeraHbaseMR(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        FILE_ROOT = args[0];
        FILE_INPUT = args[1];
        FILE_OUTPUT = args[2];
//        FILE_ROOT = "/";
//        FILE_INPUT = "gps_online_test";
//
//        FILE_OUTPUT = "gps_online";

//        if (!FILE_OUTPUT.contains(basename)) {
//            System.exit(-1);
//        }
        Configuration conf = new Configuration();
        //设置zookeeper
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //设置hbase表名称
        conf.set(TableInputFormat.INPUT_TABLE, FILE_INPUT);
        conf.set(TableOutputFormat.OUTPUT_TABLE, FILE_OUTPUT);
        Scan scan = new Scan();

        //将该值改大，防止hbase超时退出


        // 0 定义干活的人
        Job job = new Job(conf);
        job.setJobName("rowkeyoperate");
        TableMapReduceUtil.initTableMapperJob(FILE_INPUT, scan, CarmeraHbaseMRMapper.class, Text.class, Text.class, job);

        // 打包运行必须执行的方法
        job.setJarByClass(CarmeraHbaseMR.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 1.3 分区
        job.setNumReduceTasks(0);

        // 1.4 TODO 排序、分组 目前按照默认方式执行
        // 1.5 TODO 规约

        // 2.2 指定自定义reduce类
        job.setReducerClass(CarmeraHbaseMRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        // 2.3 指定写出到哪里

//        FileOutputFormat.setOutputPath(job, outpath);
//        job.setOutputFormatClass(TextOutputFormat.class);

        // 让干活的人干活

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

class CarmeraHbaseMRMapper extends TableMapper<Text, Text> {


    {

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        JSONObject jobj;
        try {
            for (Cell cell : value.rawCells()) {
                jobj = new JSONObject();
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
//                String family = Bytes.toString(CellUtil.cloneFamily(cell));
//                String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
//                String val = Bytes.toString(CellUtil.cloneValue(cell));
//                jobj.put("rowkey", rowkey);
//                jobj.put("family", family);
//                jobj.put("qualify", qualify);
//                jobj.put("val", val);
                context.write(new Text(rowkey), new Text(""));
            }
        } catch (Exception e) {
            System.out.println("出错了....");
        }
    }
}

class CarmeraHbaseMRReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void setup(Context context) {


    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String rowkey = key.toString();


    }

}
