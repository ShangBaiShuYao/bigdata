package com.shangbaishuyao.utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
/**
 * Desc: 读取kafka数据的工具类 <br/>
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 21:24 2021/3/12
 */
object MyKafkaUtil {

  //读取配置信息
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val brokers: String = properties.getProperty("kafka.broker.list")
  private val groupID: String = properties.getProperty("group.id")
  private val deserializer: String = properties.getProperty("deserializer")

  //创建Kafka参数
  private val kafkaPara: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupID,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializer, //反序列化的类
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializer
  )

  def getKafkaDStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {
    //获取Kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, topics)
    kafkaDStream
  }

}
