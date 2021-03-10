package com.shangbaishuyao.app

import java.util.Properties

import com.shangbaishuyao.datamode.UserVisitAction
import com.shangbaishuyao.handler.SingleJumpHandler
import com.shangbaishuyao.utils.{JdbcUtil, PropertiesUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
/**
 * Desc: 需求三：页面单跳转化率 <br/>
 *
 * 什么是页面单跳转换率，比如一个用户在一次 Session
 * 过程中访问的页面路径 3,5,7,9,10,21，那么页面 3 跳到页面 5 叫一次单跳，
 * 7-9 也叫一次单跳，那么单跳转化率就是要统计页面点击的概率，
 * 比如：计算 3-5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）为 A，
 * 然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，
 * 那么 B/A 就是 3-5 的页面单跳转化率。
 *
 *
需求三：页面单跳转化率
 * //1.加载数据并转换为样例类:userVisitActionRDD
 * //2.读取配置信息，获取指定的页面顺序(1,2,3,4,5,6,7)
//第一部分:统计单页点击次数
 * //3.按照指定的页面顺序过滤数据集(1,2,3,4,5,6):userVisitActionRDD[filter]
 * //4.将页面作为key，将1L作为value：userVisitActionRDD=>(page_id,1)[map]
 * //5.统计单页访问次数：(page_id,1)=>(page_id,count)[reduceByKey]
//第二部分:统计单跳次数
 * //6.转换数据结构：line=>(session,(actionTime,page_id))[map]
 * //7.按照session分组：(session,(actionTime,page_id))=>(session,Iter[(actionTime,page_id)...])[groupBykey]
 * //8.对value进行排序以及过滤(session,Iter[(actionTime,page_id)...])=>(session,from_to)
 * //9.转换数据结构：(session,from_to)=>(from_to,1)
 *
 * create by shangbaishuyao on 2021/3/9
 *
 * @Author: 上白书妖
 * @Date: 21:11 2021/3/9
 */
object SingleJumpApp {

  def main(args: Array[String]): Unit = {

    //一、加载数据并转换为样例类:userVisitActionRDD
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("CategoryTop10App").setMaster("local[*]")

    //2.创建SparkContext
    val sc = new SparkContext(conf)

    //3.读取数据
    val lineRDD: RDD[String] = sc.textFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkGmall\\sparkmall-offline\\src\\main\\resources\\data\\user_visit_action.txt", 4)

    //4.转换数据结构，将每一行数据转换为样例类对象
    val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(line => {

      val splits: Array[String] = line.split("_")

      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })

    //结果存缓存
    userVisitActionRDD.cache()

    //二.读取配置信息，获取指定的页面顺序(1,2,3,4,5,6,7)
    val properties: Properties = PropertiesUtil.load("jump.properties")
    val pageStr: String = properties.getProperty("singleJumpPage")
    val pageArr: Array[String] = pageStr.split(",")

    //获取单页数据(1,2,3,4,5,6)
    val fromPages: Array[String] = pageArr.dropRight(1)

    //获取单跳数据(1_2,2_3....)
    val toPages: Array[String] = pageArr.drop(1)
    val singleJumpPages: Array[String] = fromPages.zip(toPages).map { case (from, to) => s"${from}_$to" }

    //第一部分:统计单页点击次数((1,100),(2,200)....)
    val pageToCount: RDD[(String, Long)] = SingleJumpHandler.getFromPageCount(userVisitActionRDD, fromPages)

    //第二部分:统计单跳次数((1_2,50),(2_3,100)....)
    val singleJumpToCount: RDD[(String, Int)] = SingleJumpHandler.getJumpPageCount(userVisitActionRDD, singleJumpPages)

    //第三部分
    //11.(from_to,count)/(page_id,count)=>(from_to,ratio)
    val pageToSingleJumpToCount: RDD[(String, (String, Int))] = singleJumpToCount.map { case (fromTo, count) => (fromTo.split("_")(0), (fromTo, count)) }

    val singleJumpToRatio: RDD[(String, Double)] = pageToSingleJumpToCount.join(pageToCount).map { case (pageId, ((fromTo, fromToCount), pageCount)) =>
      (fromTo, fromToCount / pageCount.toDouble)
    }

    val taskId: Long = System.currentTimeMillis()

    //12.转换数据结构
    val result: Array[Array[Any]] = singleJumpToRatio.collect().map { case (fromTo, ratio) =>
      Array(taskId, fromTo, ratio)
    }

    //13.写库操作
    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)", result)

    //14.关闭资源
    sc.stop()
  }

}