package com.shangbaishuyao.Kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * <p>自动维护offset,将offset维护到checkPoint处 <p/>
 * Desc:对于createDirectStream 这种,他的offset存在checkPoint中,这里我们做一下checkPoint,offset保留在checkPoint中,所以每一次都是从新的地方开始消费的.
 * create by shangbaishuyao on 2021/3/11
 * @Author: 上白书妖
 * @Date: 18:57 2021/3/11
 */
object AutoCommitDirectAPITest2 {

  def main(args: Array[String]): Unit = {

    //如果我的checkPoint有了,则使用checkPoint里面的,如果没有这个依赖getSSC来创建一个
    //1.创建StreamingContext                        给一个checkPoint路径,后面是一个函数
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./checkPoint", () => getSSC)
//    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => getSSC)

    //2.启动任务
    ssc.start()
    ssc.awaitTermination()

  }



  def getSSC: StreamingContext = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("ReceiverAPITest").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.设置CheckPoint
    ssc.checkpoint("./ck1") //在本地的ck1中保留checkPoint
    //虽然在这个设置checkPoint可行,但是有个问题,就是我们没有用到这checkPoint,
    //我每一次重新他都新创建了一个StreamingContext,在这里也就是说我这里的checkPoint只是做保存数据用的,并没有去用它,也就是说
    //我每次进来都是新的StreamingContext, 不是接着上一次的StreamingContext开始消费的


    //4.Kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

    //5.读取Kafka数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaPara,
      Set("first"))

    //6.WordCount并打印
    kafkaDStream
      .flatMap { case (key, value) => value.split(" ") }
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //7.返回
    ssc
  }
}
