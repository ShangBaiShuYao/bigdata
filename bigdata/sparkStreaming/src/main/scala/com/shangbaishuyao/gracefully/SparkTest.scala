package com.shangbaishuyao.gracefully

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: 测试优雅的关闭  <br/>
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 13:57 2021/3/12
 */
object SparkTest {
  def main(args: Array[String]): Unit = {
    //从checkPoint中恢复,但是生产环境不是不从老的CheckPoint中恢复
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./checkPoint5", () => createSSC())
    //开启监控线程
    new Thread(new MonitorStop(ssc)).start()
    ssc.start()
    ssc.awaitTermination()
  }



  def createSSC(): _root_.org.apache.spark.streaming.StreamingContext = {
    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = values.sum
      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)
      Some(sum + lastStatu)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")
    //设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("./checkPoint5")
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 5555)
    val word: DStream[String] = line.flatMap(_.split(" "))

    println("*******")

    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)
    wordAndCount.print()
    ssc
  }
}

