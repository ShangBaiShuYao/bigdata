package com.atguigu.app

import java.util
import com.alibaba.fastjson.JSON
import com.shangbaishuyao.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.shangbaishuyao.constants.GmallConstants
import com.shangbaishuyao.utils.{MyElasticSearchUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization
import scala.collection.mutable.ListBuffer
/**
 * Desc:对OrderDetail的重新改造. OrderDetail有问题 <br/>
 * create by shangbaishuyao on 2021/3/19
 * @Author: 上白书妖
 * @Date: 16:07 2021/3/19
 */
object OrderDetail2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderDetailApp").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3.读取三个主题的数据创建三个流
    val kafkaOrderInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_ORDER_TOPIC))
    val kafkaOrderDetailDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
    //4.将三种数据转换为样例类对象
    val orderidToOrderInfoDStream: DStream[(String, OrderInfo)] = kafkaOrderInfoDStream.map { case (_, value) =>
      //将value转换为OrderInfo对象
      val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
      //处理手机号
      orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4) + "*******"
      //处理创建的年月以及小时
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      (orderInfo.id, orderInfo)
    }
    val orderidToOrderDetailDStream: DStream[(String, OrderDetail)] = kafkaOrderDetailDStream.map { case (_, value) =>
      val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    }
    val kafkaUserInfoDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_USER_TOPIC))
    //5.将order_info和order_detail进行join操作
    val orderJoinDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderidToOrderInfoDStream.fullOuterJoin(orderidToOrderDetailDStream)
    //6.对分区类的数据进行操作
    val noUserSaleDetailDStream: DStream[SaleDetail] = orderJoinDetailDStream.mapPartitions(iter => {
      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //定义集合，存放结合上的数据集
      val list = new ListBuffer[SaleDetail]()
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
      iter.foreach { case (order_id, (orderInfoOp, orderDetailOp)) =>
        //a.判断orderInfoOp是否为空
        if (orderInfoOp.isDefined) {
          //orderInfoOp不为空，取出orderInfoOp的值
          val orderInfo: OrderInfo = orderInfoOp.get
          //一.判断orderDetailOp是否为空
          if (orderDetailOp.isDefined) {
            //orderDetailOp不为空
            val orderDetail: OrderDetail = orderDetailOp.get
            //两表均不为空，结合数据
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //存入集合
            list += saleDetail
          }
          //二.将orderInfo转换为JSON并存入Redis
          val orderInfoKey = s"order:$order_id"
          //val str: String = JSON.toJSONString(orderInfo)//转换异常
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(orderInfoKey, orderInfoJson)
          jedisClient.expire(orderInfoKey, 300)
          //三.查询Redis中中是否有orderDetail数据
          val orderDetailKey = s"detail:$order_id"
          val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
          if (!orderDetailSet.isEmpty) {
            import scala.collection.JavaConversions._
            orderDetailSet.foreach(orderDetailJson => {
              //转换为OrderDetail的对象
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              //结合
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              //将saleDetail放入集合
              list += saleDetail
            })
          }
          //orderInfoOp为空
        } else {
          //取出orderDetailOp中的数据
          val orderDetail: OrderDetail = orderDetailOp.get
          //查询redis中orderInfo数据是否存在
          val orderInfoKey = s"order:$order_id"
          if (jedisClient.exists(orderInfoKey)) {
            val orderInfoJson: String = jedisClient.get(orderInfoKey)
            //将orderInfoJson转换为OrderInfo对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            list += saleDetail
          } else {
            //将当前的orderDetail数据写入Redis
            val orderDetailKey = s"detail:$order_id"
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(orderDetailKey, orderDetailJson)
            jedisClient.expire(orderDetailKey, 300)
          }
        }
      }
      //关闭Redis连接
      jedisClient.close()
      list.toIterator
    })
    //7.将用户信息存放至Redis
    kafkaUserInfoDStream.foreachRDD(rdd => {
      //对分区操作
      rdd.foreachPartition(iter => {
        //获取redis连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //将每一条数据写入Redis
        iter.foreach { case (_, userInfoJson) =>
          //转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          val userInfoKey = s"user:${userInfo.id}"
          jedisClient.set(userInfoKey, userInfoJson)
        }
        //关闭redis连接
        jedisClient.close()
      })
    })
    //8.对noUserSaleDetailDStream添加用户信息,查询Redis的方式
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {
      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //查询Redis中的用户信息添加至SaleDetail中
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
        //取出Redis中用户信息
        val userInfoJson: String = jedisClient.get(s"user:${noUserSaleDetail.user_id}")
        //将userInfoJson转换为样例类对象
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        //添加用户信息
        noUserSaleDetail.mergeUserInfo(userInfo)
        //返回数据
        noUserSaleDetail
      })
      //关闭连接
      jedisClient.close()
      details
    })
    saleDetailDStream.cache()
    saleDetailDStream.print
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //将订单ID拼接的订单详情ID作为ES中的ID
        val tuples: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (s"${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))
        MyElasticSearchUtil.insertBulk(GmallConstants.GMALL_ES_SALE_DETAIL_INDEX, tuples.toList)
      })
    })
    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}