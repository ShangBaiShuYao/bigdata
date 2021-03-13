package com.shangbaishuyao.app

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.handler.{AreaTop3AdHandler, BlackListHandler, DateAreaCityADToCountHandler, LastHourAdCount}
import com.shangbaishuyao.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: 消费kafka里面的数据 <br/>
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 11:37 2021/3/13
 */
object OnlineApp {
  def main(args: Array[String]): Unit = {
   //创建SparkConf
   val conf: SparkConf = new SparkConf().setAppName("OnlineApp").setMaster("local[*]")
    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //读取kafka流数据
    val kafkaDstream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set("ads_log"))
    //转换样例类对象,转换数据结构的一行对一行的,使用map
    val adsLogDStream: DStream[AdsLog] = kafkaDstream.map {
      case (key, value) =>
        //将value做切分
        val fields: Array[String] = value.split(" ")
        //在样例类对象中装数据
        AdsLog(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
    }
    //测试打印
//    adsLogDStream.print()
    //打印结果
    //AdsLog(1615606766209,华北,北京,1,3)
    //AdsLog(1615606766209,华北,北京,6,3)
    //AdsLog(1615606766209,华南,广州,3,4)
    //AdsLog(1615606766209,华南,深圳,4,3)
    //AdsLog(1615606766209,华东,上海,5,3)
    //AdsLog(1615606766209,华南,深圳,6,2)
    //AdsLog(1615606766209,华东,上海,1,6)

    //需求一：根据黑名单过滤当前数据集
    val filterAdsLogDStream: DStream[AdsLog] = BlackListHandler.filterAdsLogByBlackList(adsLogDStream, ssc.sparkContext)
    //将过滤后的数据集进行持久化
    filterAdsLogDStream.cache()
    //需求二：统计单日各个大区各城市各广告的点击次数并写入Redis
    DateAreaCityADToCountHandler.saveDateAreaCityADToCountToRedis(filterAdsLogDStream)
    //需求三：统计每天各地区 top3 热门广告并写入Redis
    AreaTop3AdHandler.saveDateAreaTop3AdToRedis(filterAdsLogDStream)
    //需求四：各广告最近 1 小时内各分钟的点击量
    LastHourAdCount.saveLastHourAdCountToRedis(filterAdsLogDStream)
    //需求一：校验单日用户点击单个广告次数，将超过100的用户加入黑名单
    BlackListHandler.checkAndAddBlackList(filterAdsLogDStream)
    //打印数据
    filterAdsLogDStream.print
    //开启job
    ssc.start()
    ssc.awaitTermination()
    }
}
