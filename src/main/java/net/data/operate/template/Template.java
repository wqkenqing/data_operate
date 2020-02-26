package net.data.operate.template;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wqkenqin on 2016/9/29.
 * Description:该类用于作为mr模板。
 */
public class Template implements Tool {
    static String FILE_ROOT = "";
    static String FILE_INPUT = "";
    static String FILE_OUTPUT = "";

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Template(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        FILE_ROOT = args[0];
        FILE_INPUT = args[1];
        FILE_OUTPUT = args[2];


        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI(FILE_ROOT), conf);
        Path outpath = new Path(FILE_OUTPUT);
        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);
        }

        // 0 定义干活的人
        Job job = new Job(conf);
        // 打包运行必须执行的方法
        job.setJarByClass(Template.class);
        // 1.1 告诉干活的人 输入流位置 读取hdfs中的文件。每一行解析成一个<k,v>。每一个键值对调用一次map函数
        FileInputFormat.setInputPaths(job, FILE_INPUT);
        // 指定如何对输入文件进行格式化，把输入文件每一行解析成键值对
        job.setInputFormatClass(TextInputFormat.class);

        // 1.2 指定自定义的map类
        job.setMapperClass(TemplateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 1.3 分区
        job.setNumReduceTasks(1);

        // 1.4 TODO 排序、分组 目前按照默认方式执行
        // 1.5 TODO 规约

        // 2.2 指定自定义reduce类
        job.setReducerClass(TemplateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 2.3 指定写出到哪里
        FileOutputFormat.setOutputPath(job, outpath);
        job.setOutputFormatClass(TextOutputFormat.class);

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

class TemplateMapper extends Mapper<LongWritable, Text, Text, Text> {


    {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    }
}

class TemplateReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void setup(Context context) {
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }

    public static <T> T jsonToGenericObject(String jsonString, TypeReference<T> tr) {
        ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        if (StringUtils.isBlank(jsonString)) {
            return null;
        } try {
            return OBJECT_MAPPER.readValue(jsonString, tr);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}