package com.shangbaishuyao.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.shangbaishuyao.bean.StartUpLog
import com.shangbaishuyao.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * Desc:  跨批次过滤数据  <br/>
 *        同批次过滤数据集<br/>
 * create by shangbaishuyao on 2021/3/14
 *
 * @Author: 上白书妖
 * @Date: 20:02 2021/3/14
 */
object DauHandler {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * 跨批次过滤数据
   * @param startUpLogDStream  读取Kafka的数据集转换为样例类对象
   * @param sc ssc.SparkContext
   */
  def filterDataByRedis(startUpLogDStream: DStream[StartUpLog] ,  sc:SparkContext)={
    /**
     * Transform:
     * Transform允许DStream上执行任意的RDD-to-RDD函数。
     * 即使这些函数并没有在DStream的API中暴露出来，
     * 通过该函数可以方便的扩展Spark API。
     * 该函数每一批次调度一次。
     * 其实也就是对DStream中的RDD应用转换。
     */
    startUpLogDStream.transform(transformFunc = {
      rdd=>{
        //每一次计算在Driver端获取连接和数据
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //创建RedisKey
        val date: String = sdf.format(new Date(System.currentTimeMillis()))
        //redisKey = dau:2019-01-22
        val  redisKey= s"DAU_$date"
        //查询当日用户登录名单
        val midSEt: util.Set[String] = jedisClient.smembers(redisKey)
        //关闭连接
        jedisClient.close()
        //广播midSet 向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用
        val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSEt)
        //过滤
        rdd.filter(startUpLog => !midSetBC.value.contains(startUpLog.mid))
      }
    })
  }

  /**
   * 同批次过滤数据集 (同批次内部去重) (分组)
   * @param filterStartUpLogDStreamByRediskey 按照Redis去重后的数据集
   */
  def filterDataByMid(filterStartUpLogDStreamByRediskey: DStream[StartUpLog])={
    //将数据转换为: startUpLog=>（mid,startUpLog）
    val midToStartUpLogDStream: DStream[(String, StartUpLog)] = filterStartUpLogDStreamByRediskey.map(mapFunc = {
      startUpLog => {
        (startUpLog.mid, startUpLog)
      }
    })
    //按照mid分组 : （mid,startUpLog）
    val midToStartUpLogDStreamGroup: DStream[(String, Iterable[StartUpLog])] = midToStartUpLogDStream.groupByKey()
    //获取组内一条数据 :（mid,startUpLog）
    val filterStartUpDStreamByGroup: DStream[StartUpLog] = midToStartUpLogDStreamGroup.map(mapFunc = {
      case (mid, startUpLog) => {
        startUpLog.head
      }
    })
    //返回同批次过滤后的数据集
    filterStartUpDStreamByGroup
  }


  /**
   * 将经过同批次处理,跨批次处理后的数据集放入redis中,给下一个批次过滤使用
   * @param filterStartUpLogDStreamByGroup  两次过滤后的数据集
   */
  def saveDateUserToRedis(filterStartUpLogDStreamByGroup: DStream[StartUpLog])={
    /**
     * foreachRDD(func)：这是最通用的输出操作，
     * 即将函数 func 用于产生于 stream的每一个RDD。
     * 其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统，
     * 如将RDD存入文件或者通过网络将其写入数据库。
     */
    filterStartUpLogDStreamByGroup.foreachRDD(foreachFunc = {
      rdd=>{
        rdd.foreachPartition(iters =>{
          //获取Redis连接
          val jedisClient: Jedis = RedisUtil.getJedisClient
          //处理数据(写入redis)
          iters.foreach(startUpLog =>{
            //单日登录用户RedisKey:    DAU:2019-01-22
            val redisKey = s"DAU_${startUpLog.logDate}"
            //放入Redis
            jedisClient.sadd(redisKey,startUpLog.mid)
          })
          //关闭redis连接
          jedisClient.close()
        })
      }
    })
  }
}
