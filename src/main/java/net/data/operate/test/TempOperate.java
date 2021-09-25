package net.data.operate.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import net.data.operate.util.CommonUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author kuiqwang
 * @emai wqkenqingto@163.com
 * @time 2021/9/24
 * @desc
 */
public class TempOperate {
    public static void main(String[] args) throws IOException {
        String jsonfile = CommonUtil.stream2String(new FileInputStream("/Users/kuiqwang/ownCloud/项目/水下隧道大数据/数据据/provice.json"), "utf8");
        JSONArray jarry = JSONArray.parseArray(jsonfile);
        JSONArray jarryResult = new JSONArray();
        for (Object obj : jarry) {
            JSONObject jobj = JSONObject.parseObject(String.valueOf(obj));
            String area = String.valueOf(jobj.get("start_area"));
            int num = area.indexOf("/");
            if (num > 0) {
                area = area.replace("\"", "");
                area=area.substring(0, num - 1);
                jobj.put("start_area", area);
                jarryResult.add(jobj);
                System.out.println(area);
            }else {
                jarryResult.add(jobj);
            }
        }
        FileOutputStream out = new FileOutputStream("/Users/kuiqwang/ownCloud/项目/水下隧道大数据/数据据/provice2.json");
        out.write(JSONObject.toJSONString(jarryResult).getBytes(StandardCharsets.UTF_8));
        out.flush();
        out.close();
    }
}
