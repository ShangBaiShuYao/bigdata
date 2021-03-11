package com.shangbaishuyao.app

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.handler.{AreaTop3AdHandler, BlackListHandler, DateAreaCityADToCountHandler, LastHourAdCount}
import com.shangbaishuyao.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OnlineApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("OnlineAPP").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setCheckpointDir("./ck") //保存数据
    //    ssc.checkpoint("./ck1")//保存数据+保存KafkaOffset+代码逻辑
    //    StreamingContext.getActiveOrCreate("./ck1")

    //3.读取Kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set("ads_log"))

    //4.转换数据集为样例类对象
    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map { case (key, value) =>
      val fields: Array[String] = value.split(" ")
      AdsLog(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
    }

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

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
