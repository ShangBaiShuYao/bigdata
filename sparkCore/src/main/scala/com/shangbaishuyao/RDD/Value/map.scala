package com.shangbaishuyao.RDD.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: map(func)案例 <br/>
 *
 * 1. 作用：返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
 * 2. 需求：创建一个1-10数组的RDD，将所有元素*2形成新的RDD
 *
 * create by shangbaishuyao on 2021/3/6
 *
 * @Author: 上白书妖
 * @Date: 23:42 2021/3/6
 */
object map {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("map(func)案例").setMaster("local[*]")

    //创建SparkContext,该对象是提交spark APP的入口
    val sc = new SparkContext(conf)

    //创建一个RDD
    val arr: RDD[Int] = sc.parallelize(1 to (10))

    //打印
    val result1: Array[Int] = arr.collect()

    //将所有元素*2
    val result2: RDD[Int] = arr.map(x => (x*2))

    //打印最终结果
    val result3: Array[Int] = result2.collect()

    //8.关闭连接
    sc.stop()
  }
}
