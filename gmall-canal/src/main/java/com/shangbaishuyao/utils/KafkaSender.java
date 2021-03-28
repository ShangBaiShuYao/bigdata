package com.shangbaishuyao.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
/**
 * Desc: kafka 发送者 <br/>
 * 将canal里面监控的数据发送至kafka
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖 
 * @Date: 0:18 2021/3/17
 */
public class KafkaSender {
    //初始化生产者
    private static KafkaProducer<String, String> kafkaProducer;
    //使用Kafka生产者发送数据至Kafka
    public static void send(String topic, String data) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        //发送数据
        kafkaProducer.send(new ProducerRecord<String, String>(topic, data));
    }
    //创建生产者对象
    private static KafkaProducer<String, String> createKafkaProducer() {
        //创建Kafka生产者配置信息
        Properties properties = PropertiesUtil.load("kafka.config.properties");
        //创建生产者
        kafkaProducer = new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }
}
