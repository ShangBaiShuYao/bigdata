package com.shangbaishuyao.app

import java.util

import com.alibaba.fastjson.JSON
import com.shangbaishuyao.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.shangbaishuyao.constants.GmallConstants
import com.shangbaishuyao.utils.{MyElasticSearchUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

/**
 * Desc: 双流join操作 <br/>
 * 涉及到双流join, 最终要的是将两个数据集合并到一起,所以在SaleDetail实例类里面已经准备好了合并两个数据集的方法了.
 * create by shangbaishuyao on 2021/3/18
 *
 * @Author: 上白书妖
 * @Date: 17:35 2021/3/18
 */
object OrderDetail {
  def main(args: Array[String]): Unit = {
    //初始化sparkConf
    val conf: SparkConf = new SparkConf().setAppName("OrderDetail").setMaster("local[*]")
    //初始化SparkContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //去读kafka三个主题里面的数据,创建三个流
    //订单数据主题
    val orderTopicDstream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_ORDER_TOPIC))
    //订单详情主题
    val orderDetailTopicDstream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_ORDER_DETAIL_TOPIC))
    //用户信息主题
    val userTopicDstream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_USER_TOPIC))
    //将三个流转换为样例类对象
    //优化和并orderInfo和OrderDetail这两个主题里面的内容
    //因为orderInfo和OrderDetail是几乎同时产生的,虽然他两又先后产生
    //但是userInfo一定是比他们早产生出来的很长一段时间.可能你购买东西两条就注册了,但是没有购买东西,两天后才购买. 这时候才产生OrderInfo和orderDetail这两张表
    //比如你的京东在两年前就注册了,知道现在用户信息一直都没变更过,那你的userInfo这表
    //而下订单和订单详情几乎是同时产生的,而用户信息明显比他们量长.
    //所以对于orderInfo,orderDetail这两张几乎同时产生的表我们使用流的join
    //对于用户信息即userInfo,我们使用另外的方式join
    //几乎同时产生的两张表使用流的join方式. 双流最多两三个批次只差. 如果相差太多,比如差了一天了.那么这种数据集我就不要了,因为我们本来就要做实时处理的.你这订单差了一天才过来我就不要了
    //订单数据主题转化为样例类对象:orderInfo
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderTopicDstream.map(mapFunc = {
      case (_, value) => {
        //将value转换为OrderInfo对象
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        //处理手机号
        //处理手机号
        orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(4) + "*******"
        //创建年月日及小时
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        //返回类型
        (orderInfo.id, orderInfo)
      }
    })
    //订单详情主题转化为样例类对象:OrderDetail
    val orderIdToOrderDetilDStream: DStream[(String, OrderDetail)] = orderDetailTopicDstream.map {
      case (_, value) => {
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      }
    }
    //用户信息主题转化为样例类对象:UserInfo
    val userInfoDStream: DStream[UserInfo] = userTopicDstream.map {
      case (_, value) => {
        JSON.parseObject(value, classOf[UserInfo])
      }
    }
    /**
     * RDD:
     *   RDD1[(1,"a"),(2,"b"),(3,"c")]
     *   RDD2[(1,"q"),(2,"w"),(3,"e")]
     *   =>
     *   RDD1.join(RDD2)[(1,(a,q)),(2,(b,w)),(3,(c,e))]
     *现在数据变化了:
     *   RDD1[(1,"a"),(2,"b"),(3,"c")]
     *   RDD2[(1,"q"),(2,"w"),(3,"e")]
     */
    //双流join的情况
    //那个order_info额order_detail进行join操作
    val orderInfoJionOrderDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderIdToOrderDetilDStream)
    //这一步的操作其实就是针对(Option[OrderInfo], Option[OrderDetail]))这两个而言的,一条一条的数据都要匹对上
    //但是这里面涉及到写redis操作,所以我们对于一条一条数据的操作应该换成分区操作
    //我们分区内去获取一下redis的一个连接,本来一条一条的map,所以我们需要换成mapPartition来分区间来连接redis操作
    //Iterator[T] => Iterator[U]
    val noUserSaleDetailDStream: DStream[SaleDetail] = orderInfoJionOrderDetailDStream.mapPartitions(mapPartFunc = {

      //iters是一个分区当中的数据
      iters => {
        //获取redis的连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //打印测试
        //        iters.foreach(println)
        //定义集合,存放结合上的数据集.即两条流同批次join上的数据集.这个集合里面要不断存放数据,所以是可变的
        val listBuffer: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]()
        //对于每一条数据的解析
        //join上的分为三种情况
        //第一种: 当前批次直接join上
        //第二种: 当前批次的order_info和前一批次的order_detailjoin上的
        //第三种: 当前批次的order_detail和前一批次的order_infojoin上的
        iters.foreach(f = {
          //[(String, (Option[OrderInfo], Option[OrderDetail]))]
          case (order_id, (orderInfoOption, oderDetailOption)) => {
            //判断orderInfoOption是否为空
            if (orderInfoOption.isDefined) {
              //如果orderInfoOption不为空,则取值出来
              val orderInfo: OrderInfo = orderInfoOption.get
              //判断oderDetailOption是否为空
              if (oderDetailOption.isDefined) {
                //oderDetailOption不为空
                //取值
                val oderDetail: OrderDetail = oderDetailOption.get
                //既然两者都不为空了,那么两者就可以结合
                //我们的saleDetail有个构造方法,直接放进去就可以了
                //量表均不为空,结合数据
                val saleDetail = new SaleDetail(orderInfo, oderDetail)
                //存入集合
                listBuffer.+=(saleDetail)
              }

              //下面是将数据加入缓存的情况
              //将orderInfo转为json并存入redis
              //redis的key和value问题: value直接使用对象转为json,那么key我们使用order加一个orderId
              var orderInfoKey = s"order:$order_id"
              //有问题,对于scala来说,我们对对象转为josn直接使用fastJSON就可以了
              //但是对于json转为对象就不可以了,只能使用json4s依赖
              //              JSON.toJSONString(orderInfo) 这种转换会出现异常
              implicit val formats = org.json4s.DefaultFormats
              val orderInfoJson: String = Serialization.write(orderInfo)
              //设置进入redis
              jedisClient.set(orderInfoKey, orderInfoJson)
              //设置过期时间
              jedisClient.expire(orderInfoKey, 300) //300秒

              //查询redis中是否有orderDetail数据
              val orderDetailKey = s"detail$order_id"
              val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
              //将上面java的set集合转为scala的set集合. 这样就可以遍历了
              import scala.collection.JavaConversions._
              orderDetailSet.foreach(
                orderDetailJson => {
                  //json转为orderDetail对象
                  val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                  //结合
                  val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                  //将saleDetail放入集合
                  listBuffer += saleDetail
                })
            } else { //orderInfoOption是为空的情况
              //取出orderDetailOption中的数据
              val orderDetail: OrderDetail = oderDetailOption.get
              //查询redis中orderInfo这个数据是否存在
              val orderInfoKey = s"order:$order_id"
              if (jedisClient.exists(orderInfoKey)) {
                val orderInfoJson: String = jedisClient.get(orderInfoKey)
                //将orderInfoJson转化为OrderInfo对象
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                listBuffer += saleDetail
              } else {
                //如果查缓存没有插到,就将自己存入缓存中
                val orderDetailKey = s"detail:$order_id"
                implicit val formats = org.json4s.DefaultFormats
                val orderDetailJson: String = Serialization.write(OrderDetail)
                jedisClient.sadd(orderDetailKey, orderDetailJson)
                jedisClient.expire(orderDetailKey,300)
              }
            }
          }
        })
        //关闭redis的连接
        jedisClient.close()
        //返回
        listBuffer.toIterator
      }
    })

    /**打印结果:
     * SaleDetail(3,1,2,2021-03-19 10:50:30,1,9,null,0,null,2452.0,荣耀10 GT游戏加速 AIS手持夜景 6GB+64GB 幻影蓝全网通 移动联通电信,2021-03-19)
     * SaleDetail(5,1,2,2021-03-19 10:50:30,1,4,null,0,null,1442.0,小米Play 流光渐变AI双摄 4GB+64GB 梦幻蓝 全网通4G 双卡双待 小水滴全面屏拍照游戏智能手机,2021-03-19)
     * SaleDetail(4,1,2,2021-03-19 10:50:30,1,10,null,0,null,222.0,小米（MI） 小米路由器4 双千兆路由器 无线家用穿墙1200M高速双频wifi 千兆版 千兆端口光纤适用,2021-03-19)
     */
    //TODO 测试打印
//    noUserSaleDetailDStream.print()
    //将用户信息放到redis做缓存
    userTopicDstream.foreachRDD(foreachFunc = {
      rdd =>{
          //对分区进行操作
          rdd.foreachPartition(
            iter=>{
              //获取redis的连接
              val jedisClient: Jedis = RedisUtil.getJedisClient
              //将每一条数据写入redis
              iter.foreach(f = {
                case (str, userInfoJson) =>{
                  //转化样例类对象
                  val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                  val userInfoKey =  s"user:${userInfo.id}"
                  jedisClient.set(userInfoKey,userInfoJson)
                }
              })
              //关闭redis的连接
              jedisClient.close()
            })
          }
       })
    //对noUserSaleDetailDStream添加用户信息,查询redis的方式
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {
      //获取redis的连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //查询redis中的用户信息,添加值SaleDetail当中
      //对分区当中每一个用户添加信息,返回数据
      val details: Iterator[SaleDetail] = iter.map(
        noUserDetail => {
          //取出redis中的用户信息
          val userInfoJson: String = jedisClient.get(s"user:${noUserDetail.user_id}")
          //将userInfoJson转化为转化为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          //添加用户信息
          noUserDetail.mergeUserInfo(userInfo)
          //返回数据
          noUserDetail
        }
      )
      //关闭redis的连接
      jedisClient.close()
      details
    })
    //结果假如缓存
    saleDetailDStream.cache()
    //测试打印
    saleDetailDStream.print
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //将订单ID拼接的订单详情ID作为ES中的ID
        val tuples: Iterator[(String, SaleDetail)] = iter.map(
          saleDetail =>
            (s"${saleDetail.order_id}-${saleDetail.order_detail_id}", saleDetail))
            MyElasticSearchUtil.insertBulk(GmallConstants.GMALL_ES_SALE_DETAIL_INDEX, tuples.toList)
      })
    })
    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
