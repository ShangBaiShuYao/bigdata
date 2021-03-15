package com.shangbaishuyao.wordCount

import com.shangbaishuyao.UDF.myReceiver
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc: 需求：自定义数据源，实现监控某个端口号，获取该端口号内容。<br/>
 *
 * 需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。
 * create by shangbaishuyao on 2021/3/11
 *
 * @Author: 上白书妖
 * @Date: 15:29 2021/3/11
 */
object ReceiverWordCount {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")

    //初始化SparkContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //加载数据创建Dstream
    val Dstream: ReceiverInputDStream[String] = ssc.receiverStream(new myReceiver("hadoop102", 9999))

    //打印
    Dstream.print()

    //开启任务
    ssc.start()
    ssc.awaitTermination() //阻塞main方法不关闭
  }
}
