package net.data.operate.mr;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * @className: HbaseTemplateMR
 * @author: kuiqwang
 * @date: 2019/1/8 9:55 AM
 * @describe: hbase to hbase
 **/
public class HbaseExport implements Tool {
    static String FILE_ROOT = "";
    static String FILE_INPUT = "";
    static String FILE_OUTPUT = "";

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new HbaseExport(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
//
//        FILE_ROOT = args[0];
//        FILE_INPUT = args[1];
//        FILE_OUTPUT = args[2];
        FILE_ROOT = "hdfs://namenode:8020/";
        FILE_INPUT = "pbu:environment_bureau-data";
        FILE_OUTPUT = "hdfs://namenode:8020/out/yd";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(FILE_ROOT), conf);
        Path outpath = new Path(FILE_OUTPUT);

        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);
        }
        //设置zookeeper
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //设置hbase表名称
        conf.set(TableInputFormat.INPUT_TABLE, FILE_INPUT);
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        //将该值改大，防止hbase超时退出

        // 0 定义干活的人
        Job job = new Job(conf);
        job.setJobName("export hbase");
        TableMapReduceUtil.initTableMapperJob(FILE_INPUT, scan, HbaseExportMapper.class, Text.class, Text.class, job);

        // 打包运行必须执行的方法
        job.setJarByClass(HbaseExport.class);

        // 1.1 告诉干活的人 输入流位置 读取hdfs中的文件。每一行解析成一个<k,v>。每一个键值对调用一次map函数
//        FileInputFormat.setInputPaths(job, FILE_INPUT);
        // 指定如何对输入文件进行格式化，把输入文件每一行解析成键值对
//        job.setInputFormatClass(TableInputFormat.class);

        // 1.2 指定自定义的map类
//        job.setMapperClass(HbaseMRMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 1.3 分区
        job.setNumReduceTasks(1);

        // 1.4 TODO 排序、分组 目前按照默认方式执行
        // 1.5 TODO 规约

        // 2.2 指定自定义reduce类
        job.setReducerClass(HbaseExportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


//        FileOutputFormat.setOutputPath(job, outpath);
//        job.setOutputFormatClass(TextOutputFormat.class);

        // 让干活的人干活
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(FILE_OUTPUT));

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

class HbaseExportMapper extends TableMapper<Text, Text> {
    String name;

    @Override
    protected void setup(Context context) {
        TableSplit split = (TableSplit) context.getInputSplit();
        name = Bytes.toString(split.getTableName());
    }


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        JSONObject jobj;
        try {

            for (Cell cell : value.rawCells()) {
                jobj = new JSONObject();
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                jobj.put("rowkey", rowkey);
                jobj.put("family", family);
                jobj.put("qualify", qualify);
                jobj.put("val", val);
                context.write(new Text(name), new Text(name + "\t" + JSONObject.toJSONString(jobj)));
            }
        } catch (Exception e) {
            System.out.println("出错了....");
        }

    }
}

@Slf4j
class HbaseExportReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs outputs;

    @Override
    public void setup(Context context) {
        outputs = new MultipleOutputs(context);
    }

    public static String changeString(String name) {
        return name.replace(":", "-");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            outputs.write(val, null, changeString(key.toString()));

        }
        log.info("name is [{}]", changeString(key.toString()));

    }

}
