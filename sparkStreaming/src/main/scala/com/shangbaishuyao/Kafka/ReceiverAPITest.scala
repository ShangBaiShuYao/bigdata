package com.shangbaishuyao.Kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: 测试zk和kafka <br/>
 *
 * create by shangbaishuyao on 2021/3/11
 * @Author: 上白书妖
 * @Date: 18:10 2021/3/11
 */
object ReceiverAPITest {
  def main(args: Array[String]): Unit = {

    //初始化SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //初始化StreamingContext
    val ssc= new StreamingContext(conf, Seconds(3))

    //kafka参数
//    val map: Map[String, String] = Map[String, String]("zookeeper.connection" -> "", "groupId" -> "bigdata")

    //读取kafka数据,创建流
    val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata",
      Map[String, Int]("first" -> 2)//主题是first 分区是2
    )

    //打印数据
    kafkaDstream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
