package com.shangbaishuyao.Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc: 实例操作 <br/>
 *
 * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
 * 需求：统计出每一个省份广告被点击次数的TOP3
 *
 * create by shangbaishuyao on 2021/3/7
 *
 * 样本如下：
 * 时间戳        省份  城市  用户    广告(AD)
 * 1516609143867  6    7     64     16
 * 1516609143869  9    4     75     18
 * 1516609143869  1    7     87     12
 *
 * @Author: 上白书妖
 * @Date: 11:26 2021/3/7
 */
object CaseTOP3 {
  def main(args: Array[String]): Unit = {
    //初始化Spark 配置信息,并设置APP名称
    val conf: SparkConf = new SparkConf().setAppName("TOP3").setMaster("local[*]")

    //创建提交Spark APP入口的SaprkContext对象
    val sc = new SparkContext(conf)

    //按行读取文本文件数据生成RDD: TS,Province,City,User,AD ; 虽然是按行读取,但是他把整个文本读出来了
    val lineRDD: RDD[String] = sc.textFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkCore\\src\\main\\resources\\Case\\agent.log")

    //获取省份,广告,并赋值1
    //按照最小细粒度聚合:((Province,AD),1)
    val provinceAdAndOne  : RDD[((String, String), Int)] = lineRDD.map(line => {
          val fields: Array[String] = line.split(" ") //将一行数据按空格切开 得到一个数组, 但是我们要的是数组的第二位和最后一位
          ((fields(1), fields(4)), 1) //但是我们元组需要一个什么样的形式呢? 原始数据: 1516609143867  6    7     64     16
                                      //通过原始数据得出我们只需要第二位和最后一位, 而且我们需要这样的形式:  (1,b)->1 即用元组表示: ((第几位数,第几位数),1)
    })

    //计算省中每个广告被点击的总数: ((Province,AD),sum)
    val provinceAdToSum : RDD[((String, String), Int)] = provinceAdAndOne.reduceByKey(_ + _) //(V,V) => V  :RDD[K,V]

    //将省份作为key，广告加点击数为value：(Province,(AD,sum))
    //val provinceToAdSum : RDD[(String, (String, Int))] = provinceAdToSum.map(x => (x._1._1, (x._1._2, x._2))) //这种方式写看不清楚,这种不是错的,只是不好理解
    val provinceToAdSum: RDD[(String, (String, Int))] = provinceAdToSum.map { case ((province, ad), sum) => (province, (ad, sum)) } //转换维度

    //将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup : RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()

    //分组内部排序并取前三
    //接下来就是组内排序了, 对Iterable[(String, Int)] 这个整体进行排序,即对迭代器然后取前三, 这个排序就是一个scala集合,scala集合排序sortWith或者sortBy都可以,sortWith需要写具体的规则
    //对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
//    val provinceAdTop3 : RDD[(String, List[(String, Int)])] = provinceGroup.mapValues(x => x.toList.sortWith((x, y) => x._2 > y._2).take(3))
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues(iters => {
      iters.toList.sortWith(_._2 > _._2).take(3) //是对Iterable[(String, Int)]这个里面取前三个; 注意: 我们要的是1号省份,2号省份,3号省份各自取三条数据, 所以我们应该在这里面取前三,不是全局取三个
    })


    //RDD[(String, List[(String, Int)])]: 然后这个list里面就只有三条数据了
    //将数据拉取到Driver端,并打印
    provinceAdTop3.collect().foreach(println)

    //将结果输出到文件
    provinceAdTop3.saveAsTextFile("H:\\IDEA_WorkSpace\\bigdata\\bigdata\\sparkCore\\src\\main\\resources\\Case\\out")

    //关闭于Spark的连接
    sc.stop()
  }
}
