package com.shangbaishuyao.handler

import com.shangbaishuyao.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
/**
 * Desc: 需求二：热门品类中活跃Session <br/>
 *
 * 需求分析:
 * 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 SessionId。
 * 这个就是说，对于Top10 的品类，每一个都要获取对它点击次数排名前 10 的 sessionId。这个功能，
 * 可以让我们看到，对某个用户群体最感兴趣的品类，
 * 各个品类最感兴趣最典型的用户的 Session 的行为。计算完成之后，将数据保存到 MySQL 数据库中。
 * 每种十条数据总共一百条数据
 * create by shangbaishuyao on 2021/3/9
 *
 * @Author: 上白书妖
 * @Date: 21:06 2021/3/9
 */
object CategoryTop10SessionHandler {

  def getCategoryTop10SessionHandler(userVisitActionRDD: RDD[UserVisitAction], categoryTop10: List[String]): RDD[(String, String, Int)] = {
    //1.按照需求一结果对原始数据过滤，找出点击了热门品类的数据:line=>line[filter]
    val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => categoryTop10.contains(userVisitAction.click_category_id.toString))

    //2.转换数据结构:line=>((category,sessionId),1)[map]
    val categorySessionToOne: RDD[((String, String), Int)] = filterUserVisitActionRDD.map { userVisitAction =>
      ((userVisitAction.click_category_id.toString, userVisitAction.session_id), 1)
    }

    //3.求和:((category,sessionId),1)=>((category,sessionId),count)[reduceByKey]
    val categorySessionToCount: RDD[((String, String), Int)] = categorySessionToOne.reduceByKey(_ + _)

    //4.转换数据结构:((category,sessionId),count)=>(category,(sessionId,count))[map]
    val categoryToSessionCount: RDD[(String, (String, Int))] = categorySessionToCount.map { case ((category, sessionId), count) =>
      (category, (sessionId, count))
    }

    //5.按照category分组:(category,(sessionId,count))=>(category,Iter[(sessionId,count),...])[groupByKey]
    val categoryToSessionCountIter: RDD[(String, Iterable[(String, Int)])] = categoryToSessionCount.groupByKey()

    //6.组内排序求Top10:(category,(sessionId,count))=>(category,Iter[(sessionId,count)*10])[mapValues]
    val categoryToSessionCountTop10: RDD[(String, List[(String, Int)])] = categoryToSessionCountIter.mapValues(iter => {

      iter.toList.sortWith { case (s1, s2) =>
        s1._2 > s2._2
      }.take(10)

    })

    //7.转换数据结构:(category,Iter[(sessionId,count)*10])=>(category,(sessionId,count))*10[flatMap]
    val category_session_count: RDD[(String, String, Int)] = categoryToSessionCountTop10.flatMap { case (category, list) =>

      list.map { case (sessionId, count) =>
        (category, sessionId, count)
      }
    }

    category_session_count
  }

}
