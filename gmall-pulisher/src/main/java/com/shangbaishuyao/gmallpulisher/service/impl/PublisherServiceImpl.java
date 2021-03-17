package com.shangbaishuyao.gmallpulisher.service.impl;

import com.shangbaishuyao.constants.GmallConstants;
import com.shangbaishuyao.gmallpulisher.mapper.DauMapper;
import com.shangbaishuyao.gmallpulisher.mapper.OrderMapper;
import com.shangbaishuyao.gmallpulisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;
    @Override
    //通过日期获取数据
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }
    @Override
    public Map getDauTotalHourMap(String date) {
        //定义Map存放分时统计的数据
        HashMap<String, Long> hourDauMap = new HashMap<>();
        //查询Pheonix获取分时数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        //遍历list:map((LOGHOUR->09),(ct->895)),map((LOGHOUR->10),(ct->1053))
        for (Map map : list) {
            hourDauMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return hourDauMap;
    }
    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }
    @Override
    public Map getOrderAmountHourMap(String date) {
        //1.获取分时统计的GMV数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        //2.构建Map接收结果数据
        HashMap<String, Double> result = new HashMap<>();
        //3.遍历list将数据转换结构存入result->list:((CREATE_HOUR->00),(SUM_AMOUNT->895.2)),map((CREATE_HOUR->01),(SUM_AMOUNT->1895.2))
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return result;
    }
    @Override
    public Map getSaleDetail(String date, int startpage, int size, String keyword) {
        //创建ES查询语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //创建BoolQueryBuilder
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //全值过滤
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        //分词匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        //设置查询过滤条件
        searchSourceBuilder.query(boolQueryBuilder);
        //设置聚合组
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(200);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);
        //分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);
        //创建Map存放结果数据
        HashMap<String, Object> resultMap = new HashMap<>();

        try {
            //执行查询
            SearchResult searchResult = jestClient.execute(new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.GMALL_ES_SALE_DETAIL_INDEX).addType("_doc").build());
            //获取总数
            Long total = searchResult.getTotal();
            //获取明细
            ArrayList<Map> detailList = new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }

            //获取聚合组数据
            MetricAggregation aggregations = searchResult.getAggregations();
            TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
            HashMap<String, Long> genderMap = new HashMap<>();
            for (TermsAggregation.Entry entry : groupby_user_gender.getBuckets()) {
                genderMap.put(entry.getKey(), entry.getCount());
            }

            TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
            HashMap<String, Long> ageMap = new HashMap<>();
            for (TermsAggregation.Entry entry : groupby_user_age.getBuckets()) {
                ageMap.put(entry.getKey(), entry.getCount());
            }

            resultMap.put("total", total);
            resultMap.put("ageMap", ageMap);
            resultMap.put("genderMap", genderMap);
            resultMap.put("detail", detailList);
        } catch (IOException e) {
               e.printStackTrace();
             }
        return resultMap;
    }
}
