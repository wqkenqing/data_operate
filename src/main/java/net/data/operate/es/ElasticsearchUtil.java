package net.data.operate.es;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class ElasticsearchUtil {

    private static TransportClient client;
    static BulkRequestBuilder bulkRequest;

    static {
        TransportClient transportClient = null;
        try {
            // 配置信息
            String clusterName = "es6.7.2";
            String hostName = "192.168.10.100,192.168.10.101,192.168.10.102";
            String port = "9300";
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName) // 集群名字
                    .put("client.transport.sniff", true)// 增加嗅探机制，找到ES集群
                    .build();
            // 配置信息Settings自定义
            transportClient = new PreBuiltTransportClient(settings);
            String ipList[] = hostName.split(",");
            for (int i = 0; i < ipList.length; i++) {
                TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(ipList[i]), Integer.valueOf(port));
                transportClient.addTransportAddresses(transportAddress);
            }

        } catch (Exception e) {

        }
        client = transportClient;

    }

    public static TransportClient getClient() {
        return client;
    }

    // 创建索引
    public static boolean createIndex(String index) {



        if (!isIndexExist(index)) {
            log.info("索引不存在!");
        }
        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(index).get();
        log.info("索引创建成功 " + indexResponse.isAcknowledged());
        return indexResponse.isAcknowledged();
    }

    public static boolean putIndexMapping(String index, String type) {
        // mapping
        XContentBuilder mappingBuilder;
        try {
            mappingBuilder = XContentFactory.jsonBuilder().startObject().field("");
        } catch (Exception e) {
            return false;
        }
        IndicesAdminClient indicesAdminClient = client.admin()
                .indices();
        AcknowledgedResponse response = indicesAdminClient
                .preparePutMapping(index).setType(type)
                .setSource(mappingBuilder).get();
        return response.isAcknowledged();
    }

    // 删除索引
    public static boolean deleteIndex(String index) {
        if (!isIndexExist(index)) {
            log.info("索引不存在!");
        }
        client.admin().indices().prepareDelete(index).get();
        log.info("索引删除成功 ");
        return true;
    }

    // 判断索引是否存在
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (inExistsResponse.isExists()) {
            log.info("索引 [" + index + "] 存在!");
        } else {
            log.info("索引 [" + index + "] 不存在!");
        }
        return inExistsResponse.isExists();
    }

    // 数据添加指定ID
    public static String addData(JSONObject jsonObject, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
//        log.info("新增数据 status:{},id:{}", response.status().getStatus(), response.getId());
        return response.getId();
    }

    // 不指定elasticsearch中的ID
    public static String addData(JSONObject jsonObject, String index, String type) {
        return addData(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    // 批量导入数据
    public static boolean bulkData(File file, String index, String type) {
        FileReader fr = null;
        BufferedReader bfr = null;
        try {
            fr = new FileReader(file);
            bfr = new BufferedReader(fr);
            String line = null;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while ((line = bfr.readLine()) != null) {
                Map inputData = JsonUtils.parse(line, Map.class);
                bulkRequest.add(client.prepareIndex(index, type).setSource(inputData));
            }
            bulkRequest.execute().actionGet();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (bfr != null) {
                    bfr.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static boolean bulkData(JSONObject jsonObject, String index, String type) {

        try {
            String line = null;
            bulkRequest.add(client.prepareIndex(index, type).setSource(jsonObject));
            bulkRequest.execute().actionGet();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // 通过ID 获取数据，fields 需要显示的字段，逗号分隔（缺省为全部字段）
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {
        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);
        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }
        GetResponse getResponse = getRequestBuilder.get();
        return getResponse.getSource();
    }

    // 通过ID 删除数据
    public static void deleteDataById(String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        log.info("删除数据 status:{},id:{}", response.status().getStatus(), response.getId());
    }

    // 通过ID 更新数据
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index).type(type).id(id).doc(jsonObject);
        client.update(updateRequest);
    }

    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param query          查询条件
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static List<Map<String, Object>> searchListData(
            String index, String type, QueryBuilder query, Integer size,
            String fields, String sortField, String highlightField) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);

        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }

        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(query);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        log.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();

        long totalHits = searchResponse.getHits().totalHits;

        long length = searchResponse.getHits().getHits().length;

        log.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;

    }

    // 高亮结果集
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<>();
        StringBuffer stringBuffer = new StringBuffer();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());
            if (StringUtils.isNotEmpty(highlightField)) {
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }
}
