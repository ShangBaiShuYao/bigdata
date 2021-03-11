package com.shangbaishuyao.Kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: 测试kafka的0-10版本,对于0-10版本,他的consumerffset(消费者偏移量)保存在系统主题里面 <br/>
 * create by shangbaishuyao on 2021/3/11
 * @Author: 上白书妖
 * @Date: 20:22 2021/3/11
 */
object KafkaTest {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "shangbaishuyao",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

    //3.读取Kafka数据创建流 ,这里就只剩下createDirectStream了
    //LocationStrategies: 位置策略
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("second"), kafkaPara))

    //4.打印
    kafkaDStream.map(x => x.value()).print

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}