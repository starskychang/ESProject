package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReaderBySourceBuilder {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://spark1:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //4.1 添加查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        boolQueryBuilder.filter(termQueryBuilder);

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "王者荣耀");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //4.2 添加聚合组
        TermsAggregationBuilder builder = AggregationBuilders.terms("groupByClass").field("class_id").size(10);
        searchSourceBuilder.aggregation(builder);

        //创建search对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        SearchResult result = jestClient.execute(search);

        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            System.out.println("************************************");
            for (Object key : source.keySet()) {
                System.out.println("key:" +key+ ", value:" +source.get(key));
            }
        }

        //7.关闭连接
        jestClient.shutdownClient();
    }


}
