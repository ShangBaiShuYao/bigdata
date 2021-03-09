package com.shangbaishuyao.Accumulator

import java.util

import org.apache.spark.util.AccumulatorV2
/**
 * Desc: 自定义累加器<br/>
 *
 * 输入类型是String类型, 输出类型是一个set集合类型
 *
 * create by shangbaishuyao on 2021/3/8
 * @Author: 上白书妖
 * @Date: 21:19 2021/3/8
 */
class MyAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{

  //定义一个空的Set集合
  val list = new util.ArrayList[String]()

  //判断当前累加器是否空 ; 我们判断当前累加器是否为空,实际上就是判断他的属性是否为空
  override def isZero: Boolean =list.isEmpty

  //复制一个新的累加器,这个方法做什么用的呢? 因为我们只是在Driver端定义好了累加器,我是不是要发给很多的Executor呀,他要复制发给其他的Executor
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    //复制一个新的就是创建一个新的
    new MyAccumulator
  }

  //重置累加器, 就是当你复制好了一个累加器之后,他会判断你当前累加器是否为空,如果不为空他就要重置
  override def reset(): Unit = list.clear()

  //给累加器添加值的
  override def add(v: String): Unit = list.add(v)

  //将多个分区的数据进行合并
  //我们要把数据放到多个Executor里面去执行,最后返回的结果是不是多个Executor汇总的结果呀,这个merge就是说你要把多个Executor里面的值给我发回来,发回来之后将多个分区的数据进行合并
  //就是将other这个内容合并到当前的List里面
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    //当前累加器里面的值和other(其他)传过来的累加器里面的值如何合并呢
    this.list.addAll(other.value)
  }

  //获取累加器中的数据
  override def value: util.ArrayList[String] = list
}
