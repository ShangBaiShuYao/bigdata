package com.shangbaishuyao.SparkCoreMysql

import org.apache.spark.{SparkConf, SparkContext}
/**
 * Desc: 往Mysql写入数据 <br/>
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 16:31 2021/3/8
 */
object WriteMysqlRDD {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("MysqlApp")
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(List("Female", "Male","Female"))

    data.foreachPartition(insertData)
  }

  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop102:3306/rdd", "root", "000000")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into rddtable(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }
}
