package com.shangbaishuyao.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: spark-yarn上的调试  <br/>
 * create by shangbaishuyao on 2021/3/5
 * @Author: 上白书妖
 * @Date: 14:17 2021/3/5
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("wordCount")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3.读取文件
    val line: RDD[String] = sc.textFile(args(0))

    //4.压平
    val word: RDD[String] = line.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOne: RDD[(String, Int)] = word.map((_, 1))

    //6.求总和
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //7.保存文件
    wordToCount.saveAsTextFile(args(1))

    //8.关闭连接
    sc.stop()
  }
}
