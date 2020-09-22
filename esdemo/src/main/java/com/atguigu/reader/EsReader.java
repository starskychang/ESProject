package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://spark1:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();


        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"male\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\":[\n" +
                "      \t{\n" +
                "      \t\t\"match\":{\n" +
                "      \t\t\t\"favo\":\"王者荣耀\"\n" +
                "      \t\t}\n" +
                "      \t}\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();

        //6.执行插入数据操作
        SearchResult result = jestClient.execute(search);


//        Long total = result.getTotal();
//        System.out.println("查询命中:" + total + "条!");

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
