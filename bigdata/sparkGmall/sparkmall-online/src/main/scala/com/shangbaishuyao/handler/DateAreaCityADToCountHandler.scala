package com.shangbaishuyao.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityADToCountHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 统计单日各个大区各城市各广告的点击次数并写入Redis
    *
    * @param filterAdsLogDStream 根据黑名单过滤后的数据集
    */
  def saveDateAreaCityADToCountToRedis(filterAdsLogDStream: DStream[AdsLog]): Unit = {

    //1.转换数据结构
    val dateAreaCityADToOne: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(adsLog => {

      //获取日期
      val date: String = sdf.format(new Date(adsLog.timestamp))

      //(日期,大区，城市，广告)，1L
      ((date, adsLog.area, adsLog.city, adsLog.adid), 1L)

    })

    //2.使用有状态转换计算累加结果
    val updateFunc: (Seq[Long], Option[Long]) => Some[Long] = (seq: Seq[Long], state: Option[Long]) => {
      //当前批次求和
      val sum: Long = seq.sum

      //取出状态中的数据
      val lastSum: Long = state.getOrElse(0L)

      //相加记录为当前批次的状态
      Some(sum + lastSum)
    }
    val dateAreaCityADToCount: DStream[((String, String, String, String), Long)] = dateAreaCityADToOne.updateStateByKey(updateFunc)

    //3.将数据写入Redis
    dateAreaCityADToCount.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取Redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //将结果写入Redis（方案一：单条数据提交）
        //        iter.foreach { case ((date, area, city, ad), count) =>
        //          val redisKey = s"Date_Area_City_AD_$date"
        //          val hashKey = s"${date}_${area}_${city}_$ad"
        //          jedisClient.hset(redisKey, hashKey, count.toString)
        //        }

        //将结果写入Redis（方案二：批量数据提交）
        // ((date, area, city, ad), count)=>(date,List[((date, area, city, ad), count)])
        val dateToDateAreaCityADToCount: Map[String, List[((String, String, String, String), Long)]] = iter.toList.groupBy(_._1._1)

        //(date,List[((date, area, city, ad), count)])=>(date,List[s"date_area_city_ad", count)])
        val dateToRedisList: Map[String, List[(String, String)]] = dateToDateAreaCityADToCount.mapValues(list => {
          list.map { case (((date, area, city, ad), count)) => (s"${date}_${area}_${city}_$ad", count.toString) }
        })

        //写入Redis（批量提交）
        dateToRedisList.foreach { case (date, list) =>
          val redisKey = s"Date_Area_City_AD_$date"
          import scala.collection.JavaConversions._
          jedisClient.hmset(redisKey, list.toMap)
        }

        //关闭连接
        jedisClient.close()

      })

    })


  }

}
