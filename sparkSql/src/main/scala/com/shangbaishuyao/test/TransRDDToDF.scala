package com.shangbaishuyao.test

import com.shangbaishuyao.bean.People
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc:
 * create by shangbaishuyao on 2021/3/10
 *
 * @Author: 上白书妖
 * @Date: 15:09 2021/3/10
 */
object TransRDDToDF {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TransRDDToDF").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //3.创建SparkSession
    //    val spark = new SparkSession(sc)
    val ss: SparkSession = SparkSession   //这里面肯定有一个sparkContext,因为SparkContext才是最根本的和spark集群的一个连接
      .builder()
      .appName("TransRDDToDF")
      .master("local[*]")
      .getOrCreate()

    //4.创建RDD
    val rdd1: RDD[String] = sc.textFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkSql\\src\\main\\resources\\people.txt")


    //5.将RDD转换ROW类型
    val peopleRDD: RDD[People] = rdd1.map(line => {
      val arr: Array[String] = line.split(",")
      //将数据填入样例类对象中 People(name: String, age: Int)  rim():函数的功能是去掉首尾空格
      People(arr(0).trim, arr(1).trim.toInt)
    })

    //6.转换为DF/DS  这个时候需要在代码级别导入隐式转换
    import ss.implicits._   //这个ss应该默认是spark的, 但是我写了不想改了
    val df: DataFrame = peopleRDD.toDF()

    //7.打印
    df.show()

    //8.关闭资源
    sc.stop()
    ss.stop()
  }
}
