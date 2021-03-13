package com.shangbaishuyao.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.utils.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis
/**
 * Desc: 需求四：各广告最近 1 小时内各分钟的点击量
 * create by shangbaishuyao on 2021/3/13
 * @Author: 上白书妖
 * @Date: 12:36 2021/3/13
 */
object LastHourAdCount {
  //时间格式化对象
  private val sdf = new SimpleDateFormat("HH:mm")
  //RedisKey
  private val redisKey = "last_hour_ads_click"
  /**
    * 各广告最近 1 小时内各分钟的点击量并写入Redis
    *
    * @param filterAdsLogDStream 根据黑名单过滤后的数据集
    */
  def saveLastHourAdCountToRedis(filterAdsLogDStream: DStream[AdsLog]) = {
    //1.adsLog=>((1,14:36),1):map
    val adHourMinToOne: DStream[((String, String), Long)] = filterAdsLogDStream.map(adsLog => {
      //获取点击的小时及分钟
      val hourMin: String = sdf.format(new Date(adsLog.timestamp))
      //返回数据
      ((adsLog.adid, hourMin), 1L)

    })
    //2.((1,14:36),1)=>((1,14:36),5):reduceByKeyAndWindow
    val adHourMinToCount: DStream[((String, String), Long)] = adHourMinToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(3))
    //3.((1,14:36),5)=>(1,(14:36,5)):map
    val adToHourMinCount: DStream[(String, (String, Long))] = adHourMinToCount.map { case ((ad, hourMin), count) => (ad, (hourMin, count)) }
    //4.(1,(14:36,5))=>(1,Iter[(14:36,5),(14:37,6)...(15:36,6)]):groupByKey
    val adToHourMinCountIter: DStream[(String, Iterable[(String, Long)])] = adToHourMinCount.groupByKey()
    //    adToHourMinCountIter.cache()
    //    adToHourMinCountIter.print
    //5.写入Redis:(2,ArrayBuffer((16:40,32), (16:39,267)))
    adToHourMinCountIter.foreachRDD(rdd => {
      //删除数据（每个批次删除一次，删除上一次计算的结果）
      val jedisClient: Jedis = RedisUtil.getJedisClient
      if (jedisClient.exists(redisKey)) {
        jedisClient.del(redisKey)
      }
      //关闭连接
      jedisClient.close()
      //删除数据之后将每个分区数据写入
      rdd.foreachPartition(iter => {
        if (iter.nonEmpty) {
          //获取连接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          val adToJsonIter: Iterator[(String, String)] = iter.map { case (ad, list) =>
            //将list装换为JSON
            import org.json4s.JsonDSL._
            val hourMinCountJson: String = JsonMethods.compact(list.toList.sortWith(_._1 < _._1))
            (ad, hourMinCountJson)
          }
          //写入Redis
          import scala.collection.JavaConversions._
          jedisClient.hmset(redisKey, adToJsonIter.toList.toMap)
          //关闭连接
          jedisClient.close()
        }
      })
    })
  }
}
