package net.data.operate.enums;


import net.data.operate.util.CommonUtil;

import java.util.Date;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2019/11/5
 * @desc
 */
public enum GatherLogEnum {
    LYDSJ_GATHER_LOG_COUNT("LYDSJ_GATHER_LOG_COUNT"),
    LYDSJ_GATHER_CALL_COUNT("LYDSJ_GATHER_CALL_COUNT"),
    LYDSJ_GATHER_CALL_ERROR("LYDSJ_GATHER_CALL_ERROR"),
    LYDSJ_GATHER_LOG_ERROR("LYDSJ_GATHER_LOG_ERROR"),
    LYDSJ_GATHER_LOG_STORE("LYDSJ_GATHER_LOG_STORE"),
    LYDSJ_GATHER_LOG_STORE_AFTERDAY("LYDSJ_GATHER_LOG_STORE_AFTERDAY"),
    LYDSJ_GATHER_LOG_LOCATION("LYDSJ_GATHER_LOG_LOCATION"),
    LYDSJ_GATHER_LOG_STATE("LYDSJ_GATHER_LOG_STATE"),
    LYDSJ_GATHER_LOG_STATE_TIME("LYDSJ_GATHER_LOG_STATE_TIME"),
    LYDSJ_GATHER_SERVER_STATE("LYDSJ_GATHER_SEVER_STATE"),
    LYDSJ_GATHER_LOG_COUNT_TOTAL("LYDSJ_GATHER_LOG_COUNT_TOTAL"),
    LYDSJ_GATHER_CALL_COUNT_TOTAL("LYDSJ_GATHER_CALL_COUNT_TOTAL"),
    LYDSJ_GATHER_CALL_ERROR_TOTAL("LYDSJ_GATHER_CALL_ERROR_TOTAL"),
    LYDSJ_GATHER_LOG_STORE_TOTAl("LYDSJ_GATHER_LOG_STORE_TOTAL"),
    LYDSJ_HOST_IP("LYDSJ_GATHER_HOST_IP"),
    LYDSJ_DATA_TYPE_MAP("LYDSJ_GATHER_SOURCE_NAME_MAP"),
    LYDSJ_DATA_SOURCE_MAP("LYDSJ_GATHER_DATA_SOURCE_MAP"),
    LYDSJ_DATA_INIT_JOB("LYDSJ_GATHER_DATA_INIT_JOB"),
    LYDSJ_DATA_BASE_TYPE("LYDSJ_GATHER_DATA_BASE_TYPE_MAP"),
    ;
    private String name;
    private String index;
    private String type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    GatherLogEnum(String name) {
        this.name = name;
    }

    GatherLogEnum(String name, String type, String index) {
        this.name = name;
        this.index = index;
        this.type = type;
    }

    public static String createDay(String key) {
        String day = CommonUtil.formatDateToString(new Date());
        key = key + "_" + day;
        return key;
    }


}
