package com.shangbaishuyao.Kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: <p>手动维护offset,手动维护不会将offset维护在检查点(checkpoint)处,一般是维护在有事务的监控系统当中<p/>
 * create by shangbaishuyao on 2021/3/11
 * @Author: 上白书妖
 * @Date: 19:48 2021/3/11
 */
object HandlerCommitDirectAPITest {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.MYSQL_JDBC ,假设我的offset是存在mysql中.那这时候我需要做的就是通过jdbc的方式把我的上一次消费的offset给他拿出来
    //    getOffset()
    val fromOffset: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long](TopicAndPartition("second", 0) -> 3)

    //4.Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

    //5.读取数据
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
      kafkaPara,
      fromOffset,
      (m: MessageAndMetadata[String, String]) => m.message())

    //6.定义一个数组存放每个批次消费的offset
    var offsetRanges = Array.empty[OffsetRange]

    //7.处理数据+保存当前的offset
    val getOffsetDStream: DStream[String] = kafkaDStream.transform(rdd => {

      //手动维护(有事物的存储系统)
      //获取offset必须在第一个调用的算子中：
      //取出offset
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd
    })

    //8.打印数据
    getOffsetDStream.foreachRDD { rdd =>

      offsetRanges.foreach(x => println(s"${x.topic}---${x.partition}---${x.fromOffset}---${x.untilOffset}"))

      rdd.foreach(println)
      println("***************************")
    }

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()


  }

}
