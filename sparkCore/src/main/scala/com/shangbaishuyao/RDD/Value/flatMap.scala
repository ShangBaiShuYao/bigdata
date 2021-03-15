package com.shangbaishuyao.RDD.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/***
 * Desc: flatMap(func)案例,压平,把一行数据变成一个一个单词 <br/>
 *
 * 作用：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）,
 *      flatmap之前是操作一个集合,现在操作一个RDD,即分布式数据集
 *
 * 需求：创建一个元素为1-5的RDD，运用flatMap创建一个新的RDD，
 *       新的RDD为原RDD的每个元素的扩展（1->1,2->1,2……5->1,2,3,4,5）
 *
 * create by shangbaishuyao on 2021/3/7
 *
 * @Author: 上白书妖
 * @Date: 0:43 2021/3/7
 */
object flatMap {
  def main(args: Array[String]): Unit = {
    //创建SparkConf,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("flatMap(func)案例,压平,把一行数据变成一个一个单词").setMaster("local[*]")

    //创建SparkContext.该对象是提交Spark APP的入口
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    //打印看看
    val result1: Array[Int] = rdd1.collect()

    //根据原RDD创建新的RDD(1->1,2->1,2,……,5->1,2,3,4,5)
    val rdd2: RDD[Int] = rdd1.flatMap(1 to _)

    //查看数据
    val result3: Array[Int] = rdd2.collect()

    //关闭连接
    sc.stop()

  }
}
