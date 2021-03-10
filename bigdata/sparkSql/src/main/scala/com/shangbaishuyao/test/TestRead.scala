package com.shangbaishuyao.test

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Desc: 测试Spark的读数据/保存数据操作 <br/>
 * create by shangbaishuyao on 2021/3/10
 * @Author: 上白书妖
 * @Date: 17:11 2021/3/10
 */
object TestRead {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("TestSparkSQL").setMaster("local[*]")

    //2.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestSparkSQL")
      .master("local[*]")
      .getOrCreate()

    //读数据
    val df1: DataFrame = spark.read.json("./aa.json")
    val df2: DataFrame = spark.read.format("json").load("./aa.json")

    //保存数据
    df1.write.json("./bb.json")
    df1.write.format("json").save("./bb.json")
    df1.write.mode(SaveMode.Append).json("./bb.json")


    val properties = new Properties()
    val frame: DataFrame = spark.read.jdbc("url", "rddTable", properties)
    spark.read.format("jdbc").option("", "").load()

  }

}
