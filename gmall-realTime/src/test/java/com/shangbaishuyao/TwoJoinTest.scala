package com.shangbaishuyao

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 测试两个RDD间的join <br/>
 * create by shangbaishuyao on 2021/3/18
 *
 * @Author: 上白书妖
 * @Date: 20:00 2021/3/18
 */
object TwoJoinTest {
  def main(args: Array[String]): Unit = {
    //初始化SparkConf
    val conf = new SparkConf().setAppName("TwoJoinTest").setMaster("local[*]")
    //初始化SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //RDD设置k-v类型的值
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, String)] = sc.parallelize(Array((1, "q"), (2, "w"), (3, "e")))

    //    val value: RDD[(Int, (String, String))] = rdd1.join(rdd2)
    //因为是左外连接,所以以我rdd1为主表,而rdd2是附表,相连接后的结果他有的数据可能有可能没有,所以用Option表示 结果如果有值则用some包裹,没有值则为none
//    val value: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
//val value: RDD[(Int, (Option[String], String))] = rdd1.rightOuterJoin(rdd2)
    val value: RDD[(Int, (Option[String], Option[String]))] = rdd1.fullOuterJoin(rdd2) //全外连接

    /**
     * rdd1.join(rdd2): //相当于1,2,3主表只要有就给你做操作,很符合订单表里面的一条数据有可能对应详情表里面的多条数据,
     * 我们需要将多条数据都关联进来,独立形成一条明细数据后再放进去. 因为这里面有商品的名称
     * (1,(a,q))
     * (2,(b,w))
     * (3,(c,e))
     * rdd1.leftOuterJoin(rdd2): //相当于rdd1是主表
     * (1,(a,Some(q)))
     * (2,(b,Some(w)))
     * (3,(c,Some(e)))
     * rdd1.rightOuterJoin(rdd2)：
     * (3,(Some(c),e))
     * (2,(Some(b),w))
     * (1,(Some(a),q))
     * rdd1.fullOuterJoin(rdd2): //全外连接 ,所有的值都要有
     * (2,(Some(b),Some(w)))
     * (1,(Some(a),Some(q)))
     * (3,(Some(c),Some(e)))
     */
//    value.foreach(f = {
//      case value=>{
//        println(value)
//      }
//    })

    //RDD设置k-v类型的值
    val rdd3: RDD[(Int, String)] = sc.parallelize(Array((1, "a"), (2, "b")))
    val rdd4: RDD[(Int, String)] = sc.parallelize(Array((1, "q"), (2, "w"), (3, "e")))

    //    val value1: RDD[(Int, (String, String))] = rdd3.join(rdd4)
    //全外连接,两边都有可能没有值的情况
    val value1: RDD[(Int, (Option[String], Option[String]))] = rdd3.fullOuterJoin(rdd4)

    /**
     * 充分证明双流join中的RDD,如果其中一个数据本应该进入本批次RDD中,但是进入RDD2中,那么join就不匹配
     * rdd3.join(rdd4):
     * (1,(a,q))
     * (2,(b,w))
     * rdd1.fullOuterJoin(rdd2): 全外连接
     * (3,(None,Some(e)))
     * (2,(Some(b),Some(w)))
     * (1,(Some(a),Some(q)))
     */
    value1.foreach(f = {
      case value=>{
        println(value)
      }
    })
  }
}
