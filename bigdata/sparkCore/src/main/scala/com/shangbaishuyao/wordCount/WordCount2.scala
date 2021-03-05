package com.shangbaishuyao.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 本地调试 Local 模式<br/>
 * create by shangbaishuyao on 2021/3/5
 * @Author: 上白书妖
 * @Date: 14:17 2021/3/5
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3.读取文件
    val line: RDD[String] = sc.textFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkCore\\src\\main\\resources\\sparkLocal\\1.txt")

    //4.压平
    val word: RDD[String] = line.flatMap(_.split(" "))

    //5.将单词映射为元组
    val wordToOne: RDD[(String, Int)] = word.map((_, 1))

    //6.求总和
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //7.保存文件
    wordToCount.saveAsTextFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkCore\\src\\main\\resources\\sparkLocal\\out")

    //8.关闭连接
    sc.stop()
  }
}
