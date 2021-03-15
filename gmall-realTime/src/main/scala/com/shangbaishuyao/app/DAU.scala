package com.shangbaishuyao.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.shangbaishuyao.bean.StartUpLog
import com.shangbaishuyao.constants.GmallConstants
import com.shangbaishuyao.handler.DauHandler
import com.shangbaishuyao.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Desc: DAU 日活跃用户<br/>
 * create by shangbaishuyao on 2021/3/14
 *
 * 我们使用SparkStreaming做了数据的分析,
 * 利用redis同批次和跨批次去重,之后将数据进行了聚合,
 * 聚合后的结果我们是要存到Hbase当中的,把数据放入到Hbase当中的话,
 * 如果自己写API的话相当麻烦,所以我们利用一个工具叫Phoenix
 *
 * @Author: 上白书妖
 * @Date: 19:35 2021/3/14
 */
object DAU {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DAU").setMaster("local[*]")
    //初始化StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //初始化时间转换对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //读取kafka数据,创建流 ,读取kafka行为日志主题, 通过用户行为判断日活
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_START_TOPIC))
    //将从kafka中读取的每一行数据转换为样例类对象
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(mapFunc = {
      case (key, value) => {
        //将数据转换为样例类对象
        val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        //获取日志中的时间戳
        val ts: Long = startUpLog.ts
        //获取年月日及小时
        val dateHour: String = sdf.format(new Date(ts))
        //切分
        val dateHoursArray: Array[String] = dateHour.split(" ")
        //赋值
        startUpLog.logDate = dateHoursArray(0)
        startUpLog.logHour = dateHoursArray(1)
        //返回值
        startUpLog
      }
    })
    //跨批次处理数据集
    val filterStartUpLogDStreamByRediskey: DStream[StartUpLog] = DauHandler.filterDataByRedis(startUpLogDStream, ssc.sparkContext)
    //同批次过滤数据集
    val filterStartUpLogDStreamByGroup: DStream[StartUpLog] = DauHandler.filterDataByMid(filterStartUpLogDStreamByRediskey)
    //结果加入缓存
    filterStartUpLogDStreamByGroup.cache()
    //将经过跨批次和同批次过滤的数据集写入Redis,给下一个批次数据集过滤使用
    DauHandler.saveDateUserToRedis(filterStartUpLogDStreamByGroup)
    //通过Phoenix写入Hbase
    filterStartUpLogDStreamByGroup.foreachRDD(foreachFunc = {
      filterStartUpLogDStreamByGroup=>{
        //Phoenix ---HBase的SQL化插件
        //把数据写入hbase+phoenix
        import org.apache.phoenix.spark._
        filterStartUpLogDStreamByGroup.saveToPhoenix(
          "gmall_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          new Configuration(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    })
    //测试打印
    kafkaDStream.map(_._2).print()
//    startUpLogDStream.print()

    //启动任务
    ssc.start()
    //阻塞
    ssc.awaitTermination()
  }
}
