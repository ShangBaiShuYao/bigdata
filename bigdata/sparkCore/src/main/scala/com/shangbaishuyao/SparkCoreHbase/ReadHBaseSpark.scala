package com.shangbaishuyao.SparkCoreHbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Desc: 从Hbase里面读取数据 <br/>
 *
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 16:33 2021/3/8
 */
object ReadHBaseSpark {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("MysqlTest").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //Hbase参数
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    conf.set(TableInputFormat.INPUT_TABLE, "fruit_spark2")

    //3.读取HBase数据创建RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    //4.打印数据
    hbaseRDD.map(x => Bytes.toInt(x._1.get())).foreach(println)

    //5.关闭资源
    sc.stop()


  }
}
