package com.shangbaishuyao.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.shangbaishuyao.bean.{CouponAlertInfo, EventInfo}
import com.shangbaishuyao.constants.GmallConstants
import com.shangbaishuyao.utils.{MyElasticSearchUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable
import scala.util.control.Breaks._
/**
 * Desc: FCW 预警日志 <br/>
 *
 * 帅选条件分析:
 * 5分钟内->开窗
 * 同一设备->mid
 * 三次及以上用不同账号登录->uid
 * 如何得知是三个不同的账号呢?我如何知道同一设备mid,5分钟内登陆里几个账号呢?
 * 就是需要按照mid进行分组.5分钟内同一个mid登陆的所有行为数据我放在一块.然后看一下uid有多少个.
 * uid有多少个可以怎么看呢? 我当前那我的mid设备,登陆uid为a的账号之后,能不能做点击流数据?能,我收藏评论都可以.
 * 所以我按照mid分组之后可能里面只有一个uid. 我按照mid进行分组之后,后面的数据是在一个迭代器里面了.
 * 我在迭代器里面再次进行uuid分组是可以的. 但是这种比较麻烦. 我可以直接把uid放在set集合里面,
 * 我最后对mid分组的迭代器里面的所有数据遍历完了之后我看set的大小就可以了
 *
 * 并领取优惠券-> coupon
 * 如果你是领取用户卷行为的,我就把他放到set里面.
 * 并且在登录到领券过程中没有浏览商品clickItem
 * 达到以上要求则产生一条预警日志。
 * 同一设备,每分钟只记录一次预警(ES)
 *
 * 总结:
 * 同一设备（分组）
 * 5分钟内（窗口）
 * 三次不同账号登录（用户）
 * 领取优惠券（行为）
 * 没有浏览商品（行为）
 * 同一设备每分钟只记录一次预警（去重）
 * create by shangbaishuyao on 2021/3/17
 *
 * @Author: 上白书妖
 * @Date: 11:56 2021/3/17
 */
object FCW {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("FCW").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //时间转换类对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //3.读取Kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_EVENT_TOPIC))
    //测试打印
//    kafkaDStream.print()
    //4.转换为样例类对象
    val midToEventInfoDStream: DStream[(String, EventInfo)] = kafkaDStream.map (mapFunc = {
      case (_, value) =>{
        val info: EventInfo = JSON.parseObject(value, classOf[EventInfo])
        //打印测试
//        println(info.evid+"============info.evid=============")
        //取出时间戳
        val ts: Long = info.ts
        //yyyy-MM-dd HH
        val dateHourStr: String = sdf.format(new Date(ts))
        //切分日期及时间
        val dateHourArr: Array[String] = dateHourStr.split(" ")
        //赋值日期及时间
        info.logDate = dateHourArr(0)
        info.logHour = dateHourArr(1)
        //返回数据
        (info.mid, info)
      }
    })
    //测试打印
//    midToEventInfoDStream.print()
    //因为我们用到了窗口操作,所以会出现窗口内有重复数据的问题
    //5.开窗并按照mid进行分组
    val midToEventInfoDStreamIter: DStream[(String, Iterable[EventInfo])] = midToEventInfoDStream.window(Seconds(30)).groupByKey()
    //测试打印
//    midToEventInfoDStreamIter.print()
    /**
     * (mid_755,ArrayBuffer(EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,19,10,47,2021-03-18,14,1616048443138), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,28,30,8,2021-03-18,14,1616048443153), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,7,1,8,2021-03-18,14,1616048443209), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,38,34,29,2021-03-18,14,1616048443171), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addCart,40,37,41,2021-03-18,14,1616048443190), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,44,16,28,2021-03-18,14,1616048443248), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,45,9,4,2021-03-18,14,1616048443207), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,50,16,46,2021-03-18,14,1616048443226), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addCart,18,20,19,2021-03-18,14,1616048443282), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,9,50,43,2021-03-18,14,1616048443241), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,9,9,11,2021-03-18,14,1616048443262), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,13,47,11,2021-03-18,14,1616048443317), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,48,36,28,2021-03-18,14,1616048443147), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,10,29,33,2021-03-18,14,1616048443166), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,10,17,4,2021-03-18,14,1616048443222), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,33,18,40,2021-03-18,14,1616048443191), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,35,38,22,2021-03-18,14,1616048443208), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addCart,17,20,13,2021-03-18,14,1616048443266), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,29,10,32,2021-03-18,14,1616048443223), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,2,5,49,2021-03-18,14,1616048443245), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,addComment,27,20,32,2021-03-18,14,1616048443301), EventInfo(mid_755,2004,gmall,shanghai,andriod,null,event,clickItem,14,47,41,2021-03-18,14,1616048443260)))
     * (mid_377,ArrayBuffer(EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,clickItem,29,18,34,2021-03-18,14,1616048461647), EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,clickItem,50,45,6,2021-03-18,14,1616048461587), EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,addFavor,7,46,0,2021-03-18,14,1616048461623), EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,clickItem,11,28,33,2021-03-18,14,1616048461604), EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,clickItem,28,29,49,2021-03-18,14,1616048461660), EventInfo(mid_377,2834,gmall,shanghai,andriod,null,event,addCart,22,31,15,2021-03-18,14,1616048461600)))
     * (mid_1141,ArrayBuffer(EventInfo(mid_1141,272,gmall,shan1xi,andriod,null,event,addCart,28,17,41,2021-03-18,14,1616048436339), EventInfo(mid_1141,272,gmall,shan1xi,andriod,null,event,addCart,6,44,24,2021-03-18,14,1616048436428), EventInfo(mid_1141,272,gmall,shan1xi,andriod,null,event,addComment,36,27,5,2021-03-18,14,1616048436360)))
     * (mid_1985,ArrayBuffer(EventInfo(mid_1985,2262,gmall,heilongjiang,ios,null,event,addComment,27,40,2,2021-03-18,14,1616048439451), EventInfo(mid_1985,2262,gmall,heilongjiang,ios,null,event,clickItem,20,18,10,2021-03-18,14,1616048439391)))
     * (mid_4771,ArrayBuffer(EventInfo(mid_4771,3298,gmall,heilongjiang,andriod,null,event,clickItem,7,33,7,2021-03-18,14,1616048444343), EventInfo(mid_4771,3298,gmall,heilongjiang,andriod,null,event,clickItem,42,29,26,2021-03-18,14,1616048444299), EventInfo(mid_4771,3298,gmall,heilongjiang,andriod,null,event,addComment,6,36,35,2021-03-18,14,1616048444285), EventInfo(mid_4771,3298,gmall,heilongjiang,andriod,null,event,addCart,32,1,22,2021-03-18,14,1616048444282)))
     * (mid_4074,ArrayBuffer(EventInfo(mid_4074,3889,gmall,shan3xi,ios,null,event,clickItem,33,26,13,2021-03-18,14,1616048449193), EventInfo(mid_4074,3889,gmall,shan3xi,ios,null,event,addCart,24,31,48,2021-03-18,14,1616048449178)))
     */
    //6.选择需要预警的mid
    val filterMidToEventInfoDStreamIter: DStream[(String, Iterable[EventInfo])] = midToEventInfoDStreamIter.filter(filterFunc = {
      case (mid, alertInfoIter) =>{
        //存放30S内统一设备ID下的UID
        val uidSet = new mutable.HashSet[String]()
        //定义标志位，默认没有点击商品行为
        var flag = true
        //遍历数据集
        breakable {
          alertInfoIter.foreach(f = {
            alertInfo => {
              //判断是否为领券行为
              if ("coupon".equals(alertInfo.evid)) {
                // 将当前用户id加入uidSet
                uidSet.add(alertInfo.uid)
//                println(alertInfo.uid)
              } else if ("clickItem".equals(alertInfo.evid)) {
                //有点击商品行为，则不做预警
                flag = false
                break()
              }
            }
          })
        }
//        println(uidSet)
        //最终返回 拉黑
        uidSet.size >= 3 && flag
      }
    })
    //测试打印
    filterMidToEventInfoDStreamIter.print()
    //7.生成预警日志
    val alertInfoDStream: DStream[CouponAlertInfo] = filterMidToEventInfoDStreamIter.map {
      case (mid, iter) =>
      //定义HashSet存放用户id
      val uidSet = new util.HashSet[String]()
      //定义HashSet存放商品ID
      val itmesSet = new util.HashSet[String]()
      //定义集合存放操作事件名称
      val eventList = new util.ArrayList[String]()
      //遍历数据集放入预警日志实例类对象中
      iter.foreach(info => {
        if ("coupon".equals(info.evid)) {
          uidSet.add(info.uid)
          itmesSet.add(info.itemid)
        }
        eventList.add(info.evid)
      })
      //生成预警日志
      CouponAlertInfo(mid, uidSet, itmesSet, eventList, System.currentTimeMillis())
    }
    //测试打印
//    alertInfoDStream.print()
    //8.alertInfoDStream转换结构
    val mindToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfoDStream.map(alertInfo => {
      //取出时间戳
      val ts: Long = alertInfo.ts
      //将ts毫秒数转化为分钟
      val min: Long = ts / 1000 / 60
      //拼接
      (s"${alertInfo.mid}_$min", alertInfo)
    })
    //9.将数据写入ES
    mindToAlertInfoDStream.foreachRDD(rdd => {
      //对于每一个分区操作
      rdd.foreachPartition(iter => {
        MyElasticSearchUtil.insertBulk(GmallConstants.GMALL_ES_ALERT_INFO_INDEX, iter.toList)
      })
    })
    //测试打印
//    midToEventInfoDStream.print
//    alertInfoDStream.print
    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
