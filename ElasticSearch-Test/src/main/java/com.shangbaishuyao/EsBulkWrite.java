package com.shangbaishuyao;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
/**
 * Desc: 批量写入用Bulk <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 21:47 2021/3/17
 */
public class EsBulkWrite {
    public static void main(String[] args) throws IOException {
        //1.创建JestClient工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建客户端的配置信息对象
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //3.设置参数
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        Student student1 = new Student();
        student1.setName("wangwu");
        student1.setSex("male");

        Student student2 = new Student();
        student2.setName("wangwu");
        student2.setSex("male");

        Student student3 = new Student();
        student3.setName("wangwu");
        student3.setSex("male");

        //5.构建多个Index对象
        Index index1 = new Index.Builder(student1).id("7").build();
        Index index2 = new Index.Builder(student2).id("8").build();
        Index index3 = new Index.Builder(student3).id("9").build();

        //6.创建批量操作的Bulk对象
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("stu")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();
        //6.执行插入数据
        jestClient.execute(bulk);
        //7.关闭连接,因为close在关闭的时候有问题,所以我们用过时的关闭方法
        jestClient.shutdownClient();

    }
}
