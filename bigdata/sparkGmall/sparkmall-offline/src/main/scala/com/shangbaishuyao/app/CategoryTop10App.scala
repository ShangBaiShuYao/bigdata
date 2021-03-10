package com.shangbaishuyao.app

import com.shangbaishuyao.accu.CategoryCountAccumulator
import com.shangbaishuyao.datamode.UserVisitAction
import com.shangbaishuyao.handler.CategoryTop10SessionHandler
import com.shangbaishuyao.utils.JdbcUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
/**
 * Desc: <p>热门品类Top10</p>   <p>热门品类中活跃Session<p/>
 *
 *   需求一：在符合条件的 session 中，获取点击、下单和支付数量排名前 10 的品类。
 *   需求二：热门品类中活跃Session
 *
 * 上白书妖补充:
 * ①就是说我们要做对点击,下单,支付,分别做排序的,支付优先级最高,这是一种办法
 * ②加权,让他支付我给权重给高一点.因为支付是购买了的. 我可以支付一次给5分,下单一次给3分,
 * 点击一次给1分. 按照这个分数,根据公司的情况做一些事情.
 * 无论我们使用哪一种形式,我们都要去求每个品类的支付,下单和点击次数.
 * 因为你有了每个品类的点击次数只有, 至于你选按照排名,还是加权,都简单了.无非就是判断, 你支付相同比较下单,
 * 下单相同比较点击次数等等. 重点在于求总和的问题.
 *
 *
 * 解释：数据中的每个 session 可能都会对一些品类的商品进行点击、下单和支付等等行为，
 * 那么现在就需要获取这些 session 点击、下单和支付数量排名前 10 的最热门的品类。
 * 也就是说，要计算出所有这些 session 对各个品类的点击、下单和支付的次数，
 * 然后按照这三个属性进行排序（可以按照点击、下单、支付的优先顺序进行排序，
 * 也可以通过权重算出综合分数进行排序），获取前 10 个品类。
 *
 * 热门前十品类的作用:
 * 这个功能，很重要，就可以让我们明白，就是符合条件的用户，他最感兴趣的商品是什么种类。这个可以让公司里的人，清晰地了解到不同层次、不同类型的用户的心理和喜好。
 * 计算完成之后，将数据保存到 MySQL 数据库中。
 *
 * create by shangbaishuyao on 2021/3/9
 *
 * @Author: 上白书妖
 * @Date: 16:35 2021/3/9
 */
object CategoryTop10App {

  def main(args: Array[String]): Unit = {

    //创建SparkConf设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("CategoryTop10App").setMaster("local[*]")

    //创建SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //读取数据
    val rdd1: RDD[String] = sc.textFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkGmall\\sparkmall-offline\\src\\main\\resources\\data\\user_visit_action.txt")

    /**
     * 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
     * 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
     * 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_6_2019-07-17 00:00:17_null_19_85_null_null_null_null_7
     */

    //转换数据结构, 将类型为RDD[String]得rdd1数据结构,将每一行数据转换为样例类对象
    val userVisitActionRDD: RDD[UserVisitAction] = rdd1.map(line => {
      //将每一行数据进行切分
      val splits: Array[String] = line.split("_")

      /**
       * 2019-07-179526070e87-1ad7-49a3-8fb3-cc741facaddf372019-07-17 00:00:02手机-1-1nullnullnullnull3
       * 2019-07-179526070e87-1ad7-49a3-8fb3-cc741facaddf482019-07-17 00:00:10null1698nullnullnullnull19
       * 2019-07-179526070e87-1ad7-49a3-8fb3-cc741facaddf62019-07-17 00:00:17null1985nullnullnullnull7
       */
      //按照索引位置和字段类型放进去
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

    //注册累加器
    val accumulator: CategoryCountAccumulator = new CategoryCountAccumulator
    sc.register(accumulator,"categoryCountAccumulator")

    //遍历数据集,利用累加器计算各个品类点击,下单,支付的次数:click_16,order_16,pay_16
    userVisitActionRDD.foreach(userVisitAction=>{
      //取出数据中的点击,订单以及支付的品类ID
      //某一个商品品类的ID
      val click_category_id: Long = userVisitAction.click_category_id
      //一次订单中所有品类的ID集合
      val order_category_ids: String = userVisitAction.order_category_ids
      //一次支付中所有品类的ID集合
      val pay_category_ids: String = userVisitAction.pay_category_ids
      //需求一完成
        //b.判断是否为点击流数据
        if (click_category_id != -1) {
          //c.向累加器写入点击数据
          accumulator.add(s"click_$click_category_id")
          //不是点击数据，则判断是否为订单数据
        } else if (!"null".equals(order_category_ids)) {
          //d.向累加器写入订单数据
          order_category_ids.split(",").foreach(order_category_id => accumulator.add(s"order_$order_category_id"))
          //如果还不是订单数据，则判断是否为支付数据
        } else if (!"null".equals(pay_category_ids)) {
          //d.向累加器写入订单数据
          pay_category_ids.split(",").foreach(pay_category_id => accumulator.add(s"pay_$pay_category_id"))
        }

    })

    //7.取出累加器中数据:(click_16->16)
    val categoryCount: mutable.HashMap[String, Long] = accumulator.value

    //测试打印
    println("==================start println acumulator value ===================")
    //    (click_8,5974)
    //    (pay_4,1271)
    //    (order_1,1766)
    //    (pay_11,1202)
    //    (order_4,1760)
    //    (click_10,5991)
    //    (pay_7,1252)
    //    (click_19,6044)
    categoryCount.foreach(println)
    println("=================over println acumulator value ===================")

    //categoryToCategoryCountMap
    //(12,Map(click_12 -> 6095, order_12 -> 1740, pay_12 -> 1218))
    //(8,Map(click_8 -> 5974, pay_8 -> 1238, order_8 -> 1736))
    //(19,Map(click_19 -> 6044, pay_19 -> 1158, order_19 -> 1722))
    //(4,Map(pay_4 -> 1271, order_4 -> 1760, click_4 -> 5961))
    //(15,Map(click_15 -> 6120, pay_15 -> 1259, order_15 -> 1672))
    //(11,Map(pay_11 -> 1202, order_11 -> 1781, click_11 -> 6093))
    //(9,Map(pay_9 -> 1230, order_9 -> 1736, click_9 -> 6045))
    //8.按照品类id分组(16,(click_16->16),(order_16,23),(pay_16,12))
    val categoryToCategoryCountMap: Map[String, mutable.HashMap[String, Long]] = categoryCount.groupBy(
      //(click_16->16)
      //_._1 等于click ====> 按照"_" 分割成了两部分=====取第一位就是16
      _._1.split("_")(1)
    )
    //(8,Map(click_8 -> 5974, pay_8 -> 1238, order_8 -> 1736))
    //(19,Map(click_19 -> 6044, pay_19 -> 1158, order_19 -> 1722)
    //9.排序,map排序就是给他toList然后sortWith
    //因为你是sortWith , 所以c1,c2使原本的即categoryToCategoryCountMap的里面的每一个元素的类型,那这个元素类型在哪看,在这Map[String, mutable.HashMap[String, Long]
    //其实这里面放着的是一组一组的每一个id的,点击订单和支付次数,Hashmap里面存的值是他Map(click_19 -> 6044, pay_19 -> 1158, order_19 -> 1722
    val categoryToCategoryCountTop10: List[(String, mutable.HashMap[String, Long])] = categoryToCategoryCountMap.toList.sortWith {
      case (c1, c2) =>
        //取出待比较对象的类别以及次数
        //16
        val category1: String = c1._1
        //[(click_16->50),(order_16,45),(pay_16,30)]
        val categoryCountMap1: mutable.HashMap[String, Long] = c1._2
        //15
        val category2: String = c2._1
        //[(click_15->50),(order_15,45),(pay_15,30)]
        val categoryCountMap2: mutable.HashMap[String, Long] = c2._2
        //先比较支付次数
        if (categoryCountMap1.getOrElse(s"pay_$category1", 0L) == categoryCountMap2.getOrElse(s"pay_$category2", 0L)) {
          //如果支付次数相同，则比较订单次数
          if (categoryCountMap1.getOrElse(s"order_$category1", 0L) == categoryCountMap2.getOrElse(s"order_$category2", 0L)) {
            //如果订单次数相同，则比较点击次数
            categoryCountMap1.getOrElse(s"click_$category1", 0L) >= categoryCountMap2.getOrElse(s"click_$category2", 0L)
          } else {
            //订单次数不同，则直接按照订单次数大小排序
            categoryCountMap1.getOrElse(s"order_$category1", 0L) > categoryCountMap2.getOrElse(s"order_$category2", 0L)
          }
        } else {
          //支付次数不同，则直接按照支付次数大小排序
          categoryCountMap1.getOrElse(s"pay_$category1", 0L) > categoryCountMap2.getOrElse(s"pay_$category2", 0L)
        }
    }.take(10) //选取前10名

    val taskId: Long = System.currentTimeMillis()

    println("======================start println categoryToCategoryCountTop10 =============================")
    categoryToCategoryCountTop10.foreach(println)
    //(4,Map(pay_4 -> 1271, order_4 -> 1760, click_4 -> 5961))
    //(15,Map(click_15 -> 6120, pay_15 -> 1259, order_15 -> 1672))
    //(7,Map(pay_7 -> 1252, order_7 -> 1796, click_7 -> 6074))
    //(20,Map(pay_20 -> 1244, order_20 -> 1776, click_20 -> 6098))
    //(8,Map(click_8 -> 5974, pay_8 -> 1238, order_8 -> 1736))
    //(16,Map(click_16 -> 5928, pay_16 -> 1233, order_16 -> 1782))
    //(17,Map(order_17 -> 1752, pay_17 -> 1231, click_17 -> 6079))
    //(9,Map(pay_9 -> 1230, order_9 -> 1736, click_9 -> 6045))
    //(12,Map(click_12 -> 6095, order_12 -> 1740, pay_12 -> 1218))
    //(11,Map(pay_11 -> 1202, order_11 -> 1781, click_11 -> 6093))
    println("======================over println categoryToCategoryCountTop10 ==============================")

    //10.转换数据结构，为写库准备
    val result: List[Array[Any]] = categoryToCategoryCountTop10.map {
      case (category, categoryCountMap) =>
        Array(taskId,
        category,
        categoryCountMap.getOrElse(s"click_$category", 0L),
        categoryCountMap.getOrElse(s"order_$category", 0L),
        categoryCountMap.getOrElse(s"pay_$category", 0L))
    }

    //11.写入数据库
    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", result)

    //12.需求一完成
    println("需求一完成")



    /*
     * Desc: 处理需求二 <br/>
     *
     * create by shangbaishuyao on 2021/3/9
     * @Author: 上白书妖
     * @Date: 19:39 2021/3/9
categoryToCategoryCountTop10:
    //(4,Map(pay_4 -> 1271, order_4 -> 1760, click_4 -> 5961))
    //(15,Map(click_15 -> 6120, pay_15 -> 1259, order_15 -> 1672))
    //(7,Map(pay_7 -> 1252, order_7 -> 1796, click_7 -> 6074))
    //(20,Map(pay_20 -> 1244, order_20 -> 1776, click_20 -> 6098))
    //(8,Map(click_8 -> 5974, pay_8 -> 1238, order_8 -> 1736))
    //(16,Map(click_16 -> 5928, pay_16 -> 1233, order_16 -> 1782))
    //(17,Map(order_17 -> 1752, pay_17 -> 1231, click_17 -> 6079))
    //(9,Map(pay_9 -> 1230, order_9 -> 1736, click_9 -> 6045))
    //(12,Map(click_12 -> 6095, order_12 -> 1740, pay_12 -> 1218))
    //(11,Map(pay_11 -> 1202, order_11 -> 1781, click_11 -> 6093))
    *
    *
需求二:
    //1.按照需求一结果对原始数据过滤,找出点击了热门品类的数据:1ine=>1ine [filter]
    //2.转换数据结构:1ine=>(( category, sessionId),1) [map]
    //3.求和: ((category, sessionId),1)=>((category, sessionId), count [reduceBykey]
    //4.转换数据结构:( category, sessionId), count)=>( category,( sessionId, count)) [map]
    //5.按照Category分组: (category,(sessionId, count))=>(category, Iter[(sessionId, count),.]) [groupBykey]
    //6.组内排序求Top10:( category,( sessionid, count))=>( category,Iter[( sessionId, count)*10]) [mapValues]
    //7.转换数据结构: category,ter( sessionId, count)*10])=>( category,( sessionId, count)*10 [flatMap或者flatMapValues]
    //最终结果
    //(1, s1, count)*10
    //(10,s1, count)*10
     */
    //0.获取前10品类的id
    val categoryTop10: List[String] = categoryToCategoryCountTop10.map(_._1)


    val category_session_count: RDD[(String, String, Int)] = CategoryTop10SessionHandler.getCategoryTop10SessionHandler(userVisitActionRDD, categoryTop10) //原始数据和累加器里面的值

    //8.拉取到Driver端进行写库操作
    val category_session_countArr: Array[(String, String, Int)] = category_session_count.collect()

    //9.转换结构
    val result2: Array[Array[Any]] = category_session_countArr.map(x => Array(taskId, x._1, x._2, x._3))

    //10.写库
    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", result2)

    //11.需求二完成
    println("需求二完成")

    //资源关闭
    sc.stop()
  }
}

