package com.shangbaishuyao.test

import org.apache.spark.sql.SparkSession

/**
 * Desc: 测试使用sparkSql连接外部hive操作 <br/>
 *
 * 只需要在resources中将/spark-lcoal/conf/hive-site.xml 放置在resources下面就好了. 他自动读取文件访问外部hive
 *
 * create by shangbaishuyao on 2021/3/10
 * @Author: 上白书妖
 * @Date: 22:04 2021/3/10
 */
object TestHive1 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestMySQLWrite")
      .enableHiveSupport()  //开启对hive的支持
      .getOrCreate()

    //2.创建表
    spark.sql("use gmall").show()
    spark.sql("show tables").show()

    //3.关闭资源
    spark.stop()

  }
}
