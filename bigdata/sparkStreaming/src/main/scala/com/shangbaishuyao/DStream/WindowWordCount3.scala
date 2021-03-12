package com.shangbaishuyao.DStream

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc: 生产环境用到的比较多 <br/>
 *  他在做reduceByKeyAndWindow 的同时进行开窗
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 12:15 2021/3/12
 */
object WindowWordCount3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./checkPoint3")
    //3.创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 6666)
    //4.将一行数据转换为（单词，1）
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_, 1))
    //5.开窗并计算WordCount
    val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,   //reduce
      (x: Int, y: Int) => x - y,   //反reduce
      Seconds(10), //开窗
      Seconds(5),  //滑动步长
      new HashPartitioner(8),//分区器
      (x: (String, Int)) => x._2 > 0//这个是过滤的函数, 不希望出现(hello,0)这种减完之后的部分,所以做一个过滤.传进来的是一个元素,元素的类型是元组
    )

    //6.打印
    wordToCountDStream.print
    //7.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
