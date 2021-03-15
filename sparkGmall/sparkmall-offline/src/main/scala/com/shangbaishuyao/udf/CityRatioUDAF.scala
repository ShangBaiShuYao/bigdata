package com.shangbaishuyao.udf

import com.shangbaishuyao.bean.CityRatio
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
/**
 * Desc: 自定义函数 , 操作计算 各个城市占比 <br/>
 * create by shangbaishuyao on 2021/3/11
 * @Author: 上白书妖
 * @Date: 12:33 2021/3/11
 */
object CityRatioUDAF extends UserDefinedAggregateFunction {

  //前三个是做初始化的
  //输入：城市名
  override def inputSchema: StructType = StructType(StructField("cityName", StringType) :: Nil)
  //中间缓存：城市->点击次数
  override def bufferSchema: StructType = StructType(StructField("cityCount", MapType(StringType, LongType)) :: Nil)
  //输出：北京21.2%，天津13.2%，其他65.6%
  override def dataType: DataType = StringType
  //函数稳定性
  override def deterministic: Boolean = true
  //buffer初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
  }
  //区内累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //取出缓存中的数据
    val cityToCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    //判断传入的城市是否存在
    if (!input.isNullAt(0)) {
      //存在，获取城市名称
      val city: String = input.getString(0)
      //对获取的数据进行累加更新原缓存中的数据
      buffer(0) = cityToCount + (city -> (cityToCount.getOrElse(city, 0L) + 1L))
    }
  }
  //区间累加数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //第一个分区中的缓存数据
    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    //第二个分区中的缓存数据
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    //将分区的数据进行结合
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (city, count)) => map + (city -> (map.getOrElse(city, 0L) + count))
    }
  }
  //最终的计算---buffer:北京->50，天津->23..
  override def evaluate(buffer: Row): String = {
    //取出缓存中的城市及点击次数
    val cityToCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    //总点击次数
    var total_count = 0L
    cityToCount.foreach { case (_, count) => total_count += count }
    //排序取点击次数前2名的城市
    val cityToCountTop2: List[(String, Long)] = cityToCount.toList.sortWith(_._2 > _._2).take(2)
    //其他城市总占比
    var otherRatio = 100D
    //求出前2名的城市占比
    val result: List[CityRatio] = cityToCountTop2.map {
      case (city, count) => val cityRatio: Double = Math.round(count * 10000D / total_count) / 100D
        otherRatio -= cityRatio
        CityRatio(city, cityRatio)
    }
    //将其他城市总占比加入该集合
    val ratios: List[CityRatio] = result :+ CityRatio("其他", Math.round(otherRatio * 10D) / 10D)
    //北京21.2%，天津13.2%，其他65.6%
    ratios.mkString(",")
  }
}
