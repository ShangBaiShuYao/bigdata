package com.shangbaishuyao.utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
/**
 * Desc: 读取kafka的数据 <br/>
 * create by shangbaishuyao on 2021/3/14
 * @Author: 上白书妖
 * @Date: 18:06 2021/3/14
 */
object MyKafkaUtil {
  /**
   * @param ssc  StreamingContext
   * @param topics kafka消费主题
   * @return
   */
  def getKafkaDStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {
    //1.读取配置信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    val brokerList: String = properties.getProperty("kafka.broker.list")
    val groupId: String = properties.getProperty("kafka.group.id")
    //2.封装Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId)
    //3.读取Kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaPara,
      topics)
    //4.返回数据
    kafkaDStream
  }


}
