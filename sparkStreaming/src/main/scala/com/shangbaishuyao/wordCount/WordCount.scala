package com.shangbaishuyao.wordCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc: 测试SparkStream的Demo <br/>
 * create by shangbaishuyao on 2021/3/11
 *
 * @Author: 上白书妖
 * @Date: 14:45 2021/3/11
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //初始化spark 配置信息
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingDemo").setMaster("local[*]")

    //初始化SparkStreamingContext               Seconds(3) 代表你这二批次处理处理多大,这个是3秒计算一次
    val ssm: StreamingContext = new StreamingContext(conf, Seconds(3))

    //通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams: ReceiverInputDStream[String] = ssm.socketTextStream("hadoop102", 9999)

    //将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    //将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    //将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext
    ssm.start()  //开启这个SparkStreamingContext
    ssm.awaitTermination() //阻塞,不让这个面方法退出
  }
}
