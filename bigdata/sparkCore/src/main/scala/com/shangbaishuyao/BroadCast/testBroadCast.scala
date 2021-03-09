package com.shangbaishuyao.BroadCast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 广播变量 <br/>
 * create by shangbaishuyao on 2021/3/8
 *
 * @Author: 上白书妖
 * @Date: 22:36 2021/3/8
 */
object testBroadCast {
  def main(args: Array[String]): Unit = {
    //初始化Spark 配置信息,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("TOP3").setMaster("local[*]")

    //创建提交Spark APP入口的SaprkContext对象
    val sc = new SparkContext(conf)

    //定义广播变量
    val broadCast: Broadcast[String] = sc.broadcast("a")

    //创建RDD
    val rdd1: RDD[String] = sc.parallelize(Array("hello", "word", "dsp", "shanghai"))

    //使用广播变量, 最关键的是广播变量可以在Executor端去使用
    rdd1.filter(x =>{
       //获取广播变量
      val value: String = broadCast.value

      //判断我的数组中是否包含广播变量的值
      x.contains(value)
    }).foreach(println)

    //关闭资源
    sc.stop()
  }
}
