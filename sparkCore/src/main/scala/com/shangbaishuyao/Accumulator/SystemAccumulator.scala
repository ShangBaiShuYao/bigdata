package com.shangbaishuyao.Accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 系统累加器 <br/>
 * create by shangbaishuyao on 2021/3/8
 *
 * @Author: 上白书妖
 * @Date: 18:27 2021/3/8
 */
object SystemAccumulator {
  def main(args: Array[String]): Unit = {
    //初始化Spark 配置信息,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")

    //创建提交Spark APP入口的SaprkContext对象
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    //注册累加器
    val longAccumulator: LongAccumulator = sc.longAccumulator("accumulator")

    //其实在生产环境中,累加器我这种用法不对, 他得放在行动算子里面,不要放在转换算子里面
    //遍历数据,并使用累加器
    //统计这个数据中比2大的总个数有几个
    rdd1.map(x => { //这个数据是在遍历所有的元数,而且是区内去遍历, 第一个区只有1,2  第二个是3,4
      if (x > 2) {
        longAccumulator.add(1L) //比二大,累加1
      }
      x //这个x 是写出去的意思, 我们原来的数据不动弹,只是做一个累加器的累加
    }).collect()

    //去除累加器的值
    println(longAccumulator.value)

    //关闭连接
    sc.stop()
  }
}
