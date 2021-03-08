package com.shangbaishuyao.SparkCoreHbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Desc: 向Hbase里面写数据 <br/>
 *
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 16:36 2021/3/8
 */
object WriteHBaseSpark {
  def main(args: Array[String]): Unit = {
    //获取Spark配置信息并创建与spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    //创建HBaseConf
    val conf: Configuration = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "fruit_spark2")

    //定义往HBase插入数据的方法
    def convert(triple: (Int, String, Int)): (Any, Put) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD: RDD[(Int, String, Int)] = sc.parallelize(List((1, "apple", 11), (2, "banana", 12), (3, "pear", 13)))

    //将RDD内容写到HBase
    val localData: RDD[(Any, Put)] = initialRDD.map(convert)

    localData.saveAsHadoopDataset(jobConf)

    //关闭资源
    sc.stop()

  }
}
