package net.data.operate.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Writer;

public class JsonUtils {

    private static ObjectMapper mapper = new ObjectMapper();

    public static String toJsonString(Object obj) {
        try {
            return  mapper.writer().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void toJsonWriter(Object obj, Writer writer) {
        try {
            mapper.writeValue(writer, obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <T> T parse(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> T parse(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
