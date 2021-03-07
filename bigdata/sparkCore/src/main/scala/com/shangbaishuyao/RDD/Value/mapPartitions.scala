package com.shangbaishuyao.RDD.Value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: mapPartitions(func)案例 <br/>
 *
 * 作用：类似于map，但独立地在RDD的每一个分片上运行，
 *       因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U]。
 *       假设有N个元素，有M个分区，那么map的函数的将被调用N次(元素数),
 *       而mapPartitions被调用M次(分区数),一个函数一次处理所有分区。
 *
 * 需求：创建一个RDD，使每个元素*2组成新的RDD
 *
 * 总结:
 * 这种效率比上面map那种效率更高. 因为scala集合中的map计算过程比spark中的map算子计算过程更短.因为spark的map算子封装的东西很多
 *
 * mapPartitions是将一个分区的数据加载到内存中形成一个迭代器.所以在内存使用中,
 * mapPartitions效率高,但是耗费资源, 而map是操作一个个元素. 所以内存占用不大
 * 所以内存足够情况下,我们使用mapPartitions, 以为他一次操作一个分区.
 *
 *
 * 这里的x代表是区内所有数据  , 这个map就是我们scala的map了
 *
 * create by shangbaishuyao on 2021/3/7
 *
 * @Author: 上白书妖
 * @Date: 0:00 2021/3/7
 */
object mapPartitions {
  def main(args: Array[String]): Unit = {
    //创建sparkConf并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("mapPartitions(func)案例 ").setMaster("local[*]")

    //创建SparkContext,该对象是提交spark APP的入口
    val sc = new SparkContext(conf)

    //创建一个RDD
    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    //使每个元素*2组成新的RDD               注意:这里面的map方法不是spark的map方法,而是scala的
    val rdd2: RDD[Int] = rdd1.mapPartitions(x => x.map(x => x * 2))

    //打印
    val result1: Array[Int] = rdd2.collect()

    //关闭连接
    sc.stop()
  }
}
