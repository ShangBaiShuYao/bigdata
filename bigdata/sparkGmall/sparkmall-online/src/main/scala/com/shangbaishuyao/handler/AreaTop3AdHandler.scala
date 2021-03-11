package com.shangbaishuyao.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.handler.DateAreaCityADToCountHandler.sdf
import com.shangbaishuyao.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaTop3AdHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 统计每天各地区 top3 热门广告并写入Redis
    *
    * @param filterAdsLogDStream 根据黑名单过滤后的数据集
    */
  def saveDateAreaTop3AdToRedis(filterAdsLogDStream: DStream[AdsLog]) = {

    //1.((date,area,ad),1):map
    val dateAreaAdToOne: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(adsLog => {

      //获取日期
      val date: String = sdf.format(new Date(adsLog.timestamp))

      //(日期,大区，城市，广告)，1L
      ((date, adsLog.area, adsLog.adid), 1L)

    })

    //2.((date,area,ad),sum):updateStateByKey
    val updateFunc: (Seq[Long], Option[Long]) => Some[Long] = (seq: Seq[Long], state: Option[Long]) => {
      //当前批次求和
      val sum: Long = seq.sum

      //取出状态中的数据
      val lastSum: Long = state.getOrElse(0L)

      //相加记录为当前批次的状态
      Some(sum + lastSum)
    }
    val dateAreaAdToCount: DStream[((String, String, String), Long)] = dateAreaAdToOne.updateStateByKey(updateFunc)

    //3.(date,area),(ad,sum):map
    val dateAreaToAdCount: DStream[((String, String), (String, Long))] = dateAreaAdToCount.map { case ((date, area, ad), count) =>
      ((date, area), (ad, count))
    }

    //4.(date,area),Iter[(ad,sum)...]:groupByKey
    val dateAreaToAdCountIter: DStream[((String, String), Iterable[(String, Long)])] = dateAreaToAdCount.groupByKey()

    //5.(date,area),Iter[(ad,sum)*3]:mapValues
    val dateAreaToTop3AdCount: DStream[((String, String), List[(String, Long)])] = dateAreaToAdCountIter.mapValues(iter => {

      //排序取前三名
      iter.toList.sortWith(_._2 > _._2).take(3)
    })

    //6.将前三名点击的广告数据转换为JSON
    val dateAreaToJson: DStream[((String, String), String)] = dateAreaToTop3AdCount.mapValues(list => {
      import org.json4s.JsonDSL._
      JsonMethods.compact(list)
    })

    //7.写入Redis:foreachRDD
    dateAreaToJson.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //写Redis操作（单条数据操作）
        //        iter.foreach { case ((date, area), json) =>
        //          val redisKey = s"top3_ads_per_day:$date"
        //          jedisClient.hset(redisKey, area, json)
        //        }

        //写Redis操作（批量数据操作）
        //        (date,(area,json))
        val dateToAreaJson: Iterator[(String, (String, String))] = iter.map { case ((date, area), json) => (date, (area, json)) }

        //    (date,Iter[(date,(area,json))...])
        val dateTodateToAreaJson: Map[String, List[(String, (String, String))]] = dateToAreaJson.toList.groupBy(_._1)

        //  (date,map[area,json])
        val dateToAreaJsonMap: Map[String, Map[String, String]] = dateTodateToAreaJson.mapValues(list => {
          list.map(_._2).toMap
        })

        //写入Redis
        dateToAreaJsonMap.foreach { case (date, areaToJsonMap) =>
          val redisKey = s"top3_ads_per_day:$date"
          import scala.collection.JavaConversions._
          jedisClient.hmset(redisKey, areaToJsonMap)
        }

        //关闭连接
        jedisClient.close()

      })


    })

  }

}
