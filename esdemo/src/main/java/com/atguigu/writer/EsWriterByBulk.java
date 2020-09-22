package com.atguigu.writer;

import com.atguigu.bean.Student;
import com.atguigu.bean.Student2;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByBulk {

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
        Student2 stu1 = new Student2("0921", "watermelon");
        Student2 stu2 = new Student2("0922", "apple");
        Student2 stu3 = new Student2("0923", "basket");

        Index index1 = new Index.Builder(stu1).index("student2").type("_doc").id("1004").build();
        Index index2 = new Index.Builder(stu2).index("student2").type("_doc").id("1005").build();
        Index index3 = new Index.Builder(stu3).index("student2").type("_doc").id("1006").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("student2")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();

        //6.执行插入数据操作
        jestClient.execute(bulk);

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
