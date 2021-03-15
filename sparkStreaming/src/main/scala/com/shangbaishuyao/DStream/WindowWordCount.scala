package com.shangbaishuyao.DStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
/**
 * Desc: 测试开窗<br/>
 *
 *  开窗Windows,他不会任何处理,以前你是一个批次的数据封装成一个RDD,现在多个批次,者多个批次就看你窗口大小了,假如说是3个批次大小=窗口大小的话
 *       他就把你三个批次的数据拿过来,封装成一个RDD.操作的时候还是操作RDD,只不过数据集大小变了 <br/>
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 11:56 2021/3/12
 */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("WindowWordCount").setMaster("local[*]")
    //创建SparkSreaming
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //创建DStream
    val DStream1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //开窗,窗口大小设置为批次两倍,滑动步长为批次大小 , 开这个window只是将里面的数据集变多了,不改变数据里面的任何结构
    val windowDstream: DStream[String] = DStream1.window(Seconds(10), Seconds(5))//窗口大小10秒,滑动步长5秒
    //5.计算WordCount
    val wordToCountDStream: DStream[(String, Int)] = windowDstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //6.打印
    wordToCountDStream.print
    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
