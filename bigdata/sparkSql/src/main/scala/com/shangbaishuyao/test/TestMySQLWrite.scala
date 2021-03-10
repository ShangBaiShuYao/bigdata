package com.shangbaishuyao.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/**
 * Desc: 测试写入mysql表 <br/>
 * create by shangbaishuyao on 2021/3/10
 * @Author: 上白书妖
 * @Date: 20:12 2021/3/10
 */
object TestMySQLWrite {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestMySQLWrite")
      .getOrCreate()

    import spark.implicits._

    //2.创建DF
    val rdd: RDD[(Int, String)] = spark.sparkContext.parallelize(Array((4, "weihong"), (5, "banzhang")))

    //3.转换为DF
    val df: DataFrame = rdd.toDF("id", "name")

    //4.将数据写入MySQL
    //    val properties = new Properties()
    //    properties.setProperty("user", "root")
    //    properties.setProperty("password", "000000")
    //    df.write
    //      .mode(SaveMode.Append)
    //      .jdbc("jdbc:mysql://hadoop102:3306/rdd", "rddTable", properties)

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/sparksql")
      .option("dbtable", "rddTable3")
      .option("user", "root")
      .option("password", "000000")
      .save()

    //5.关闭资源
    spark.stop()

  }

}
