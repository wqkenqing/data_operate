package net.data.operate.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.mapred.RowCounter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-07-04
 * @desc
 */
@Slf4j
public class BashTest {

    public void testBash() {
        //测试java bash命令

    }

    public static void main(String args[]) {
        Process process = null;
        List<String> processList = new ArrayList<String>();
//        String command = "/Users/wqkenqing/Desktop/test.sh";
//        String command = "wc -l /Users/wqkenqing/Desktop/说明.txt";
        String command = args[0];
        log.info("ok");

        try {
            process = Runtime.getRuntime().exec(command);
            int exitValue = process.waitFor();
            if (0 != exitValue) {
                System.out.println("call shell failed. error code is :" + exitValue);
            }
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        for (String line : processList) {
            System.out.println(line);
        }

    }

}
