package com.shangbaishuyao;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;
/**
 * Desc: 写入数据到es <br/>
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 21:46 2021/3/17
 */
public class EsWrite {
    public static void main(String[] args) throws IOException {
        //1.创建JestClient工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        //2.创建客户端的配置信息对象
//        HttpClientConfig httpClientConfig = new HttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200"));
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //3.设置参数
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        Student student = new Student();
        student.setName("dsx");
        student.setSex("male");

        /**
         PUT stu/_doc/1
         {
         "name":"dsp",
         "sex":"1"
         }
         */
        //这个builder方法里面的source实际上就是这个大括号的json
        //5.构建Index对象
        Index index = new Index.Builder(student)
                .index("stu")
                .type("_doc")
                .id("2")
                .build();
//        Index index = new Index.Builder(" {\n" +
//                "         \"name\":\"dsp\",\n" +
//                "         \"sex\":\"1\"\n" +
//                "         }")
//                .index("stu")
//                .type("_doc")
//                .id("3")
//                .build();

        //6.执行插入数据
        jestClient.execute(index);
        //7.关闭连接
        jestClient.shutdownClient();
    }
}
