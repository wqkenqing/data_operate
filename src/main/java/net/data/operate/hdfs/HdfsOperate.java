package net.data.operate.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-04-30
 * @desc hdfs操作
 */
@Slf4j
public class HdfsOperate {
    //1. 获取文件列表 2. 创建文件名与写入数据
    //3. 创建目录    4. 读文件 5.下载hdfs文件到本地目录 6.将本地目录或文件上传的hdfs
    //7. 将本地目录或文件上传的hdfs
    Configuration conf = new Configuration();
    FileSystem fs = null;

    /**
     * 获得FileSystem
     */

    public FileSystem getFs(String path) {
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "never");
        try {
            fs = FileSystem.get(URI.create(path), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public List<String> getFileList(String path) {
        List<String> flist = new ArrayList<>();
        Configuration conf = new Configuration();
        Path path1 = new Path(path);

        try {
            FileSystem fs = path1.getFileSystem(conf);
            if (fs.exists(path1) && fs.isDirectory(path1)) {
                for (FileStatus status : fs.listStatus(path1)) {
                    flist.add(status.getPath().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flist;
    }

    public boolean writeFileToHdfs(String path) {
        Configuration conf = new Configuration();
        Path path1 = new Path(path);
        String content = "测试内容";
        try {
            System.out.println("开始测试");
            FileSystem fs = path1.getFileSystem(conf);
            FSDataOutputStream out = fs.create(path1);
            log.info("开始输出..");
            out.write(content.getBytes());
            out.close();
            log.info("输出完成..");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    //创建文件 写数据 删除文件 上传文件 下载文件 断点续写
    //创建文件夹

    /**
     * 创建文件夹
     */
    public void createPath(String path, String tag) {

        try {
            FileSystem fs = getFs(path);
            boolean res = fs.mkdirs(new Path(path));
            log.info(path + "创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 罗列path
     */
    public void listPath(String path) throws IOException {

        FileSystem fs = getFs(path);
        FileStatus[] fss = fs.listStatus(new Path(path));

        Arrays.asList(fss).forEach(f -> {
                    String fpath = f.getPath().toString();
                    String ff[] = fpath.split("/");
                    String fname = ff[ff.length - 1];
                    System.out.println(fname);
                }
        );
    }


    /**
     * 创建文件
     */
    public FSDataOutputStream createFile(String path) throws IOException {
        FileSystem fs = getFs(path);
        FSDataOutputStream fout = fs.create(new Path(path));
        log.info(path + ":创建成功...");
        return fout;
    }

    /**
     * write and append
     */
    public void createAndWrite(String path, String file) throws IOException {
        FileSystem fs = getFs(path);
        Path path1 = new Path(path);
        if (fs.exists(path1)) {
            log.info("续写中...");
            FSDataOutputStream fout = fs.append(path1);
            fout.write(file.getBytes());
            fout.close();
        } else {
            FSDataOutputStream fout = createFile(path);
            fout.write(file.getBytes());
            fout.close();
        }

        log.info("输出成功...");
    }


    public FSDataInputStream getReadInputStream(String path) {

        fs = getFs(path);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(path));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return in;
    }

    /**
     * 删除文件夹
     */
    public void deleteFile(String path) throws IOException {
        FileSystem fs = getFs(path);
        boolean res = fs.delete(new Path(path));
        if (res) {
            log.info(path + "删除成功....");
        } else {
           log.info(path + "删除失败....");

        }
    }

    public static void main(String[] args) throws IOException {

        String path = "hdfs://namenode:9000/";
        HdfsOperate operate = new HdfsOperate();
        String path1 = path + "test/test5";
//        operate.createPath(path + "test/test4", "");
//        operate.listPath(path);
//        operate.deleteFile(path + "test");
//        String file = stream2String(new FileInputStream("/Users/wqkenqing/Desktop/kuiq_wang/bigdata/bigdata/src/main/resource/doc/demo1.txt"), "utf8");
//        operate.createAndWrite(path1, file);
//        operate.readAndDownload(path1);
//        String[] ss = {"1", "2", "3"};
//        List<String> slist = Arrays.asList(ss);
//        slist.add("33");
//        System.out.println(slist.size());
        String path2 = "hdfs://ynamenode:8020/test.md";

    }

}
