package com.shangbaishuyao.UDAF

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:测试spark SQL <br/>
 * create by shangbaishuyao on 2021/3/10
 *
 * @Author: 上白书妖
 * @Date: 14:45 2021/3/10
 */
object avgAge {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并且设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("helloWord").setMaster("local[*]")

    //创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)

    //创建新版本的SparkSQL的入口
    val ss: SparkSession = SparkSession.builder().appName("helloWord").master("local[*]").getOrCreate()

    //4.读取JSON文件创建DF
    val df: DataFrame = ss.read.json("G:\\存储\\桌面\\people")

    //5.DSL风格
    println("*************DSL*************")
    //    df.select("name").show()

    //6.SQL风格
    println("*************SQL*************")
    df.createTempView("t1")

    //自定义UDAF注册函数
    ss.udf.register("MyUDAF", new MyUDAF)

    ss.sql("select MyUDAF(age) from t1").show()

    //7.关闭连接
    ss.stop()
    sc.stop()
  }
}
