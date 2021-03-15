package com.shangbaishuyao.DStream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc: Transform 将DStream转换为RDD进行操作 <br/>
 * create by shangbaishuyao on 2021/3/11
 *
 * @Author: 上白书妖
 * @Date: 21:38 2021/3/11
 */
object Transform2 {
  def main(args: Array[String]): Unit = {
    //初始化sparkConf
    val conf: SparkConf = new SparkConf().setAppName("Transform").setMaster("local[*]")

    //初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //这个是全局,只在Driver当中执行一次
    print("*****************************")

    //创建Dstream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //将DStream转化为RDD操作,将一个批次数据封装成一个RDD, 现在我直接对RDD进行操作
    val wordsAndCountDStream: DStream[(String, Int)] = lineDStream.transform(line => {

      //这个是在Executor端,而且每一行数据都会执行一次,因为这个函数整体是要传给RDD,要知道关于RDD的操作才会传给Executor端去
      print("******************************")
      //我也能在这块写一个广播变量

      //对RDD创建连接
      line.foreachPartition(iter=>{
        //创建连接

        //使用连接
        iter.foreach(
          //在foreach里面去使用连接
          println
        )
        //等foreach结束之后,关闭连接

      })

      val words: RDD[String] = line.flatMap(_.split(" "))
      val wordsAndOne: RDD[(String, Int)] = words.map(x=>{
        (x, 1)
      })
      val value: RDD[(String, Int)] = wordsAndOne.reduceByKey(_ + _)
      value
    })

    //打印
    wordsAndCountDStream.print()

    //启动
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()
  }
}
