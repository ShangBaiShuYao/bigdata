package com.shangbaishuyao.RDD.Partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Desc: 测试修改分区规则,即自定义分区器 <br/>
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 15:48 2021/3/8
 */

object partitionTest {
  def main(args: Array[String]): Unit = {
    //初始化Spark 配置信息,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("TOP3").setMaster("local[*]")

    //创建提交Spark APP入口的SaprkContext对象
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("a", 2), ("b", 3), ("b", 4)))

    //修改分区规则,重新分区 , 这里我给他分两个去,但是实际上这里分两个和一个是样的,因为我在MyPartitions里面写死了,直接返回0的.
    val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new MyPartitions(2))

//    (0,(a,1))
//    (0,(a,2))
//    (0,(b,3))
//    (0,(b,4))
    //查看结果,我们需要将他的分区号也带出来查看       //items是一个分区的数据
    rdd2.mapPartitionsWithIndex((index,items)=>{
      items.map(x =>(index,x))
    }).foreach(println)

    //关闭连接
    sc.stop()
  }
}
