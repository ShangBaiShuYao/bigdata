package com.shangbaishuyao.handler

import com.shangbaishuyao.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
/**
 * Desc: 需求三：页面单跳转化率 <br/>
 * create by shangbaishuyao on 2021/3/9
 * @Author: 上白书妖
 * @Date: 22:06 2021/3/9
 */
object SingleJumpHandler {


  def getJumpPageCount(userVisitActionRDD: RDD[UserVisitAction], singleJumpPages: Array[String]): RDD[(String, Int)] = {

    //1.转换数据结构：line=>(session,(actionTime,page_id))[map]
    val sessionToTimePage: RDD[(String, (String, String))] = userVisitActionRDD.map { userVisitAction =>
      (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id.toString))
    }

    //2.按照session分组：(session,(actionTime,page_id))=>(session,Iter[(actionTime,page_id)...])[groupBykey]
    val sessionToTimePageIter: RDD[(String, Iterable[(String, String)])] = sessionToTimePage.groupByKey()

    //3.对value进行按照时间排序
    val sessionToPageSorted: RDD[(String, List[String])] = sessionToTimePageIter.mapValues(iter => {
      iter.toList.sortWith(_._1 < _._1).map(_._2)
    })

    //4.转换数据结构(session, List[pageId])=>(session,from_to)
    val sessionToSingleJumpPage: RDD[(String, String)] = sessionToPageSorted.flatMapValues(list => {

      val fromPages: List[String] = list.dropRight(1)
      val toPages: List[String] = list.drop(1)

      fromPages.zip(toPages).map { case (from, to) => s"${from}_$to" }

    })

    //5.过滤(session,Iter[(actionTime,page_id)...])=>(session,from_to)
    val sessionToSingleJumpPageFilter: RDD[(String, String)] = sessionToSingleJumpPage.filter { case (session, fromTo) => singleJumpPages.contains(fromTo) }

    //6.转换数据结构：(session,from_to)=>(from_to,1)
    val singleJumpToOne: RDD[(String, Int)] = sessionToSingleJumpPageFilter.map(x => (x._2, 1))

    //7.求和：(from_to,1)=>(from_to,count)
    val singleJumpToCount: RDD[(String, Int)] = singleJumpToOne.reduceByKey(_ + _)

    //8.返回结果
    singleJumpToCount
  }


  def getFromPageCount(userVisitActionRDD: RDD[UserVisitAction], fromPages: Array[String]): RDD[(String, Long)] = {

    //1.按照指定的页面顺序过滤数据集(1,2,3,4,5,6):userVisitActionRDD[filter]
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => {

      fromPages.contains(userVisitAction.page_id.toString)

    })

    //2.将页面作为key，将1L作为value：userVisitActionRDD=>(page_id,1)[map]
    val pageToOne: RDD[(String, Long)] = filterUserVisitActionRDD.map(userVisitAction => (userVisitAction.page_id.toString, 1L))

    //3.统计单页访问次数：(page_id,1)=>(page_id,count)[reduceByKey]
    val pageToCount: RDD[(String, Long)] = pageToOne.reduceByKey(_ + _)

    //4.返回结果
    pageToCount
  }

}
