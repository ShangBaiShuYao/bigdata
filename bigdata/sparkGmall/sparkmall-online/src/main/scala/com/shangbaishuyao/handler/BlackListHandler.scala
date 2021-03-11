package com.shangbaishuyao.handler

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date

import com.shangbaishuyao.bean.AdsLog
import com.shangbaishuyao.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  //黑名单的key
  private val blackList = "black_list"

  /**
    * 根据黑名单过滤当前数据集
    *
    * @param adsLogDStream 原始数据集
    */
  def filterAdsLogByBlackList(adsLogDStream: DStream[AdsLog], sc: SparkContext): DStream[AdsLog] = {

    //4.方案四
    adsLogDStream.transform(rdd => {

      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //获取黑名单数据
      val uidSet: util.Set[String] = jedisClient.smembers(blackList)

      //广播uidSet
      val uidSetBC: Broadcast[util.Set[String]] = sc.broadcast(uidSet)

      //关闭连接
      jedisClient.close()

      //根据黑名单过滤数据集
      rdd.filter(adsLog =>
        !uidSetBC.value.contains(adsLog.userid)
      )
    })

    //3.方案三
    //    adsLogDStream.transform(rdd => {
    //      rdd.mapPartitions(iter => {
    //        //获取连接
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        //执行过滤操作
    //        val filterAdsLog: Iterator[AdsLog] = iter.filter(adsLog => !jedisClient.sismember(blackList, adsLog.userid))
    //        //关闭连接
    //        jedisClient.close()
    //        //返回值
    //        filterAdsLog
    //      })
    //    })

    //2.方案二
    //    adsLogDStream.filter(adsLog => {
    //      //获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //过滤
    //      val bool: Boolean = !jedisClient.sismember(blackList, adsLog.userid)
    //      //关闭连接
    //      jedisClient.close()
    //      //返回值
    //      bool
    //    })

    //1.方案一
    //    adsLogDStream.filter(adsLog => {
    //      //获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //获取黑名单
    //      val uidSet: util.Set[String] = jedisClient.smembers(blackList)
    //      //过滤
    //      val bool: Boolean = !uidSet.contains(adsLog.userid)
    //      //关闭连接
    //      jedisClient.close()
    //      //返回值
    //      bool
    //    })

  }


  /**
    * 校验单日用户点击单个广告次数，将超过100的用户加入黑名单
    *
    * @param adsLogDStream 过滤后的数据集
    */
  def checkAndAddBlackList(adsLogDStream: DStream[AdsLog]): Unit = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    //1.转换数据结构 ads_log=>((date,user,ad),1L)
    val dateUserAdToOne: DStream[((String, String, String), Long)] = adsLogDStream.map(adsLog => {

      //将时间戳转换为日期
      val date: String = sdf.format(new Date(adsLog.timestamp))

      //返回值
      ((date, adsLog.userid, adsLog.adid), 1L)
    })

    //2.统计单日用户点击单个广告的总次数
    val dateUserAdToCount: DStream[((String, String, String), Long)] = dateUserAdToOne.reduceByKey(_ + _)

    //3.将数据写入Redis并校验
    dateUserAdToCount.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //获取jedis
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //写入redis并做点击次数校验
        iter.foreach { case ((date, user, ad), count) =>

          //将单日用户点击广告次数写入
          val redisKey = "date_user_ad_count"
          val hashKey = s"$date-$user-$ad"
          jedisClient.hincrBy(redisKey, hashKey, count)

          //          if (!jedisClient.exists(hashKey)) {
          //            jedisClient.set(hashKey, count.toString)
          //            jedisClient.setex(hashKey, 60 * 60, "aa")
          //          } else {
          //            jedisClient.incrBy(hashKey, count)
          //          }
          //          val long: Long = jedisClient.get(hashKey).toLong

          //校验
          val dateUserAdCount: Long = jedisClient.hget(redisKey, hashKey).toLong
          if (dateUserAdCount >= 249L) {

            //超过100次，则将该用户拉黑
            jedisClient.sadd(blackList, user)
          }
        }

        //关闭连接
        jedisClient.close()
      })
    })

  }

}
