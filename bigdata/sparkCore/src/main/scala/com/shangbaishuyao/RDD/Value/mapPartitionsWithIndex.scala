package com.shangbaishuyao.RDD.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:  mapPartitionsWithIndex(func)案例(此index是分区号,到底你操作的是哪个分区,判别你到底操作那个分区的)	<br/>
 *
 * 作用：类似于mapPartitions，但func带有一个整数参数表示分片的索引值，
 *       因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]；
 *
 * 需求：创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
 *
 *
 * 总结:
 *     map()和mapPartition()的区别:
 *        1. map()：每次处理一条数据。
 *        2. mapPartition()：每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM。
 *        3. 开发指导：当内存空间较大的时候建议使用mapPartition()，以提高处理效率。
 *
 *
 * create by shangbaishuyao on 2021/3/7
 *
 * @Author: 上白书妖
 * @Date: 0:18 2021/3/7
 */
object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //创建sparkConf并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("mapPartitionsWithIndex(func)案例").setMaster("local[*]")

    //创建SparkContext,该对象是提交spark APP的入口
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    //使得每个元素跟所在分区形成一个元组,组成一个新的RDD
    val rdd2 = rdd1.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))

//    (3,4)
//    (1,2)
//    (2,3)
//    (0,1)
    //打印看看
    rdd2.foreach(println)

    //关闭连接
    sc.stop()
  }
}
