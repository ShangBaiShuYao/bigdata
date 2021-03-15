package com.shangbaishuyao.DStream

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
/**
 * Desc: 生产环境用到的比较多 <br/>
 *  他在做reduceByKeyAndWindow 的同时进行开窗
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 12:15 2021/3/12
 */
object WindowWordCount2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./checkPoint2")
    //3.创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //4.将一行数据转换为（单词，1）
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))
    //5.开窗并计算WordCount
    val windowToCountDstream: DStream[(String, Int)] = wordToOneDStream.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y, Seconds(10), Seconds(5)
    )
    //6.打印
    windowToCountDstream.print
    //7.启动任务
    ssc.start()
    ssc.awaitTermination()


  }

}
