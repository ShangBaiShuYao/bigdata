package com.shangbaishuyao.app

import com.alibaba.fastjson.JSON
import com.shangbaishuyao.bean.OrderInfo
import com.shangbaishuyao.constants.GmallConstants
import com.shangbaishuyao.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Desc: GMV 交易额 <br/>
 *
 * 这个数据是从canal里面发送过来的, 我们canal写了一个kafkaSender, 就是将canal获取的数据库数据发送通过kafka发送者发送至kafka中,
 * 然后由kafka消费者消费kafka数据 , 这里就是消费kafka里面的数据,进行分析
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 10:20 2021/3/17
 */
object GMV {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("GMV").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //3.读取Kafka数据创建流
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.GMALL_ORDER_TOPIC))
    //打印日志
    kafkaDStream.print()
    //4.将数据转换为样例类对象：手机号脱敏+生成数据的日期及小时
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(mapFunc = {
      case (key,value)=>{
          //将数据转换为样例类对象
          val info: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
          //手机号脱敏
          val pre3AndOthers: (String, String) = info.consignee_tel.splitAt(3)
          val last4Num: (String, String) = pre3AndOthers._2.splitAt(4)
          //拼接新的手机号
          info.consignee_tel = s"${pre3AndOthers._1}****${last4Num._2}"
          //创建日期和小时
          val create_time: String = info.create_time //yyyy-MM-dd HH:mm:ss
          val dateHourArr: Array[String] = create_time.split(" ")
          info.create_date = dateHourArr(0)
          info.create_hour = dateHourArr(1).split(":")(0)
          info
      }
    })
    //5.将数据写入HBase
    orderInfoDStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_ORDER_INFO",
        Seq(
          "ID",
          "PROVINCE_ID",
          "CONSIGNEE",
          "ORDER_COMMENT",
          "CONSIGNEE_TEL",
          "ORDER_STATUS",
          "PAYMENT_WAY",
          "USER_ID",
          "IMG_URL",
          "TOTAL_AMOUNT",
          "EXPIRE_TIME",
          "DELIVERY_ADDRESS",
          "CREATE_TIME",
          "OPERATE_TIME",
          "TRACKING_NO",
          "PARENT_ORDER_ID",
          "OUT_TRADE_NO",
          "TRADE_BODY",
          "CREATE_DATE",
          "CREATE_HOUR"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    //6.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
