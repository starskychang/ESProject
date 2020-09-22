package com.atguigu.writer;

import com.atguigu.bean.Student;
import com.atguigu.bean.Student2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://spark1:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        //5.构建ES插入数据对象
        Student stu = new Student("0317", "apple");
        Student2 stu2 = new Student2("0920", "watermelon");

        Index index = new Index.Builder(stu2).index("student2").type("_doc").id("1004").build();

//        Index index = new Index.Builder("{\n" +
//                "  \"class_id\":\"0317\",\n" +
//                "  \"stu_id\":\"006\",\n" +
//                "  \"name\":\"changwei\",\n" +
//                "  \"sex\":\"male\",\n" +
//                "  \"birth\":\"1992-08-08\",\n" +
//                "  \"favo\":\"篮球,骑行\"\n" +
//                "}").index("student").type("_doc").id("1006").build();

//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"002\",\n" +
//                "  \"movie_name\":\"006\",\n" +
//                "}").index("movie_test1").type("_doc").id("1002").build();

        //6.执行插入数据操作
        jestClient.execute(index);

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
