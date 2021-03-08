package com.shangbaishuyao.SparkCoreMysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 从Mysql读取数据<br/>
 * create by shangbaishuyao on 2021/3/8
 *
 * @Author: 上白书妖
 * @Date: 16:29 2021/3/8
 */
object ReadMysqlRDD {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "xww2018"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `rddtable` where `id`>=? and `id`<=?;",
      1,
      10,
      1,
      r => (r.getInt(1), r.getString(2))
    )

    //打印最后结果
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()
  }
}
