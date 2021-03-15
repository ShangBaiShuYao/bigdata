package com.shangbaishuyao.wordCount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
 * Desc: 实例操作 <br/>
 *
 * 需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
 * create by shangbaishuyao on 2021/3/11
 *
 * @Author: 上白书妖
 * @Date: 15:08 2021/3/11
 */
object testRDDStream {
  def main(args: Array[String]): Unit = {
    //初始化Spark配置信息
    val conf: SparkConf = new SparkConf().setAppName("testRDDStream").setMaster("local[*]")

    //初始化SparkStreaming
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //创建RDD队列
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    //创建QueueInputDStream
    val rddDstream: InputDStream[Int] = ssc.queueStream(queue,oneAtATime = false)

    //累加求和
    val sumDstream: DStream[Int] = rddDstream.reduce(_ + _)

    //打印
    sumDstream.print()

    //开启任务
    ssc.start()

    //向队列中放RDD
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(2000)
    }

    //阻塞main线程
    ssc.awaitTermination()

  }
}
