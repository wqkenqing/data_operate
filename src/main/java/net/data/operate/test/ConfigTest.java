package net.data.operate.test;

import net.data.operate.util.CommonUtil;

import java.util.Properties;
import java.util.Set;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019/10/10
 * @desc
 */
public class ConfigTest {
    public static void main(String[] args) {
        Properties properties = CommonUtil.initConfig("hbase.properties");
        Set<Object> kset = properties.keySet();
        for (Object k : kset) {
            System.out.println(properties.getProperty((String) k));

        }
    }
}
