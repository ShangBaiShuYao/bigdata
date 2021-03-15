package com.shangbaishuyao.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
/**
 * Desc: 使用连接池创建对redis的连接 <br/>
 * create by shangbaishuyao on 2021/3/12
 * @Author: 上白书妖
 * @Date: 21:31 2021/3/12
 */
object RedisUtil {
  var jedisPool: JedisPool = _

  def main(args: Array[String]): Unit = {
    val jedisClient: Jedis = getJedisClient
    jedisClient.set("key", "value")
    jedisClient.close()
  }

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val properties: Properties = PropertiesUtil.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: String = properties.getProperty("redis.port")
      val password: String = properties.getProperty("redis.password")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      //设置完属性之后去获取一个连接池
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt,600,password)
    }
    //从连接池中获取连接
    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    println("获得一个连接")
    jedisPool.getResource
  }
}

