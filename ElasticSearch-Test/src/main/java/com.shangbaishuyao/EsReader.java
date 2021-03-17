package com.shangbaishuyao;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
/**
 * Desc: 从es里面读数据,查询数据 <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 21:46 2021/3/17
 */
public class EsReader {
    public static void main(String[] args) throws IOException {
        //1.创建JestClient工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建客户端的配置信息对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //3.设置参数
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        //构建查询语句的对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //bool关键字
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //filter关键字
        boolQueryBuilder.filter(new TermQueryBuilder("sex", "female"));
        //must关键字
        boolQueryBuilder.must(new MatchQueryBuilder("favo", "铅球"));
        searchSourceBuilder.query(boolQueryBuilder);

        //aggs
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("group_by_class_id", ValueType.LONG).field("class_id").size(2));
        searchSourceBuilder.aggregation(new MaxAggregationBuilder("group_by_age").field("age"));

        //分页
//        searchSourceBuilder.from(0);
//        searchSourceBuilder.size(2);

        Search search1 = new Search.Builder(searchSourceBuilder.toString()).build();
        //5.构建Search对象
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"female\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"铅球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_class_id\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    },\n" +
                "    \"group_by_age\":{\n" +
                "      \"max\": {\n" +
                "        \"field\": \"age\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}").build();

        //6.查询数据
        SearchResult searchResult = jestClient.execute(search1);

        //7.解析searchResult
        System.out.println("命中条数：" + searchResult.getTotal() + "条！！！");
        //将整个大的"hist":[{},{},{}]的json数组转化为map,第二位void不需要管它,直接拿map做操作
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

        //解析hits数据
        for (SearchResult.Hit<Map, Void> hit : hits) {
            JSONObject jsonObject = new JSONObject();
            Map source = hit.source;
            for (Object o : source.keySet()) {
                jsonObject.put((String) o, source.get(o));
            }
            jsonObject.put("index", hit.index);
            jsonObject.put("type", hit.type);
            jsonObject.put("id", hit.id);

            System.out.println(jsonObject.toString());
        }

        /**
         * 输出结果:
         * 解析聚合数据!!!
         * 9001->3
         * 9002->1
         * 最大年纪:18.0
         */
        //解析聚合数据
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation group_by_class_id = aggregations.getTermsAggregation("group_by_class_id");

        System.out.println("解析聚合数据！！！");
        List<TermsAggregation.Entry> buckets = group_by_class_id.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println(bucket.getKey() + "-->" + bucket.getCount());
        }

        MaxAggregation group_by_age = aggregations.getMaxAggregation("group_by_age");
        System.out.println("最大年纪：" + group_by_age.getMax());
    }
}
