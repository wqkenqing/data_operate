package net.data.operate.util;

import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019-08-06
 */
public class PropertyUtil {
     static Properties properties;

    public static Properties initConfig(String path) {
        properties = new Properties();
        try {
            properties.load(System.class.getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }


}
