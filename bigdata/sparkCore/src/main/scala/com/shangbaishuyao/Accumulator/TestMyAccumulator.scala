package com.shangbaishuyao.Accumulator

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 测试自定义累加器 <br/>
 * create by shangbaishuyao on 2021/3/8
 *
 * @Author: 上白书妖
 * @Date: 22:03 2021/3/8
 */
object TestMyAccumulator {
  def main(args: Array[String]): Unit = {
    //初始化Spark 配置信息,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[*]")

    //创建提交Spark APP入口的SaprkContext对象
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd1: RDD[String] = sc.parallelize(Array("hello", "shangbaishuyao", "hadoop", "word"))

    //初始化自定义累加器
    val myAccu = new MyAccumulator

    //注册累加器
    sc.register(myAccu, "myAccu")

    //使用累加器
    rdd1.foreach(x =>{
      if(x.contains("a")){
        //将包含a元素的添加进累加器
        myAccu.add(x)
      }
      println(x)
    })


    println("====================================下面是累加器里面的值====================================")

    //取出累加器中的值
    val list: util.ArrayList[String] = myAccu.value

    //导入一个隐式转换,将Java的list变成一个Scala的集合
    import collection.JavaConversions._
    //获取list里面的每一个元素
    for(value:String <- list){
       println(value)
    }

    //关闭连接
    sc.stop()
  }
}
