package com.shangbaishuyao.mock


import java.util.{Properties, Random}

import com.shangbaishuyao.datamode.CityInfo
import com.shangbaishuyao.utils.{PropertiesUtil, RanOpt, RandomOptions}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

/**
 * Desc: 模拟生产数据 <br/>
 * 模拟生产数据,完成之后扔到kafka里面,就是通过kafka生产者,往kafka里面去发送数据,发送完成之后做一个打印
 * 数据的格式:
 * timestamp area city userid adid
 *
 *
 * 我们处理的时候需要将他转成一个样例类对象来进行处理的
 *
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 14:37 2021/3/9
 */
object MockerRealTime {
  /**
    * 模拟的数据
    *
    * 格式 ：timestamp area city userid adid
    * 某个时间点 某个地区 某个城市 某个用户 某个广告
    */
  def generateMockData(): Array[String] = {
    val array: ArrayBuffer[String] = ArrayBuffer[String]()
    val CityRandomOpt = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(1, "上海", "华东"), 30),
      RanOpt(CityInfo(1, "广州", "华南"), 10),
      RanOpt(CityInfo(1, "深圳", "华南"), 20),
      RanOpt(CityInfo(1, "天津", "华北"), 10))

    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {

      val timestamp: Long = System.currentTimeMillis()
      val cityInfo: CityInfo = CityRandomOpt.getRandomOpt
      val city: String = cityInfo.city_name
      val area: String = cityInfo.area
      val adid: Int = 1 + random.nextInt(6)
      val userid: Int = 1 + random.nextInt(6)

      // 拼接实时数据
      array += timestamp + " " + area + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    new KafkaProducer[String, String](prop)
  }

  def main(args: Array[String]): Unit = {
    // 获取配置文件commerce.properties中的Kafka配置参数
    val config: Properties = PropertiesUtil.load("config.properties")
    val broker: String = config.getProperty("kafka.broker.list")
    val topic = "ads_log"
    // 创建Kafka消费者
    val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)
    while (true) {
      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (line <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(2000)
    }
  }
}
